import osu 
import dotenv
import sys
import os
import json
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime, timedelta, timezone
import time
import threading
from flask import Flask, render_template

dotenv.load_dotenv()
class DataHandle:
    MAIN_TABLE_BLUEPRINT = (
        "CREATE TABLE `{}` ("
        "   `map_id` int(11) NOT NULL,"
        "   `playcount` int(11) NOT NULL,"
        "   `day` date NOT NULL,"
        "   `mapset_id` int(11),"
        "   `h` int(11) NOT NULL,"
        "   PRIMARY KEY (`map_id`, `day`, `h`)"
        ") ENGINE=InnoDB" )
    META_TABLE_BLUEPRINT = (
        "CREATE TABLE `{}` ("
        "   `name` varchar(32) NOT NULL,"
        "   `curse` varchar(32),"    
        "   PRIMARY KEY (`name`)"
        ") ENGINE=InnoDB" )
    MAP_TABLE_BLUEPRINT = (
        "CREATE TABLE `{}` ("
        "   `mapset_id` int(11) NOT NULL,"
        "   `name` varchar(64) NOT NULL,"
        "   `expiry` date NOT NULL,"
        "   PRIMARY KEY (`mapset_id`)"
        ") ENGINE=InnoDB" )

    def __init__(self):
        self.C_SECRET = os.getenv("CLIENT_SECRET")
        self.C_ID = os.getenv("CLIENT_ID")
        self.CLIENT = osu.Client.from_credentials(self.C_ID, self.C_SECRET, "")
        self.DB_NAME = "osuplaycount"
        self.DB_USERNAME = os.getenv("DB_USERNAME")
        self.DB_HOST = os.getenv("DB_HOST")
        self.DB_PORT = os.getenv("DB_PORT")
        self.DB_PASSWORD = os.getenv("DB_PASSWORD")
        self.DB_CNX = mysql.connector.connect(host=self.DB_HOST, user=self.DB_USERNAME, database=self.DB_NAME, port=self.DB_PORT, password=self.DB_PASSWORD)
        self.DB_CURSE = self.DB_CNX.cursor()
        self.active = False

        # Load the database
        #self._load_db()
        self._load_tables()

    def _load_tables(self):
        try:
            self.DB_CURSE.execute(DataHandle.META_TABLE_BLUEPRINT.format("osumetatable"))
            # If that doesn't fail, we should populate it with the temp data as well
            add_meta = ("INSERT INTO osumetatable "
                        "(name, curse) "
                        "VALUES (%s, %s)")
            data_meta = ("OsuMetaData", None)
            self.DB_CURSE.execute(add_meta, data_meta)
            self.DB_CNX.commit()
            print("Meta table created and populated")
        except:
            print("Meta table exists")

        try:
            self.DB_CURSE.execute(DataHandle.MAIN_TABLE_BLUEPRINT.format("osumaintable"))
            print("Main table created")
        except:
            print("Main table exists")

        try:
            self.DB_CURSE.execute(DataHandle.MAP_TABLE_BLUEPRINT.format("osumaptable"))
            print("Map table created")
        except:
            print("Map table exists")

    def _get_beatmap_info(self, limit):
        # Check the beatmaps which don't have info associated
        check_info = ("SELECT map_id FROM osumaintable "
                      "WHERE mapset_id IS NULL "
                      "ORDER BY playcount DESC "
                      "LIMIT %s")
        self.DB_CURSE.execute(check_info, [limit])

        my_results = self.DB_CURSE.fetchmany(limit)
        while len(my_results) > 0 and self.active: # If we terminate the program we should exit this
            map_id = my_results.pop()[0]
            map_info = self.CLIENT.get_beatmap(map_id)

            update_map = ("UPDATE osumaintable "
                          "SET mapset_id = %s "
                          "WHERE map_id = %s")
            data_map = (map_info.beatmapset_id, map_id)
            self.DB_CURSE.execute(update_map, data_map)
            self.DB_CNX.commit()

            # Update the maptable by adding the data
            # We should check if the entry already exists
            check_map_table = ("SELECT * FROM osumaptable "
                               "WHERE mapset_id = %s "
                               "LIMIT 1")
            self.DB_CURSE.execute(check_map_table, [map_info.beatmapset_id])
            map_table_result = self.DB_CURSE.fetchone()
            # Doesn't exist = add it
            if map_table_result is None:
                update_map_table = ("INSERT INTO osumaptable "
                                   "(mapset_id, name, expiry) "
                                   "VALUES (%s, %s, %s)")
                # Set an expiry date to be 2 days from now
                data_map_table = (map_info.beatmapset_id, map_info.beatmapset.title[:64], datetime.now(timezone.utc).date() + timedelta(days=2))
                self.DB_CURSE.execute(update_map_table, data_map_table)
                self.DB_CNX.commit()
                print("Added new map", data_map_table, "now looking for trouble.")
            else:
                # Does exist: extend the expiry
                update_map_table = ("UPDATE osumaptable "
                                    "SET expiry = %s "
                                    "WHERE mapset_id = %s")
                data_map_table = (datetime.now(timezone.utc).date() + timedelta(days=2), map_info.beatmapset_id)
                self.DB_CURSE.execute(update_map_table, data_map_table)
                self.DB_CNX.commit()
                print("Updated expiry of existing map", map_info.beatmapset.title[:64])
            
            time.sleep(1)

    
    def _process_recent_scores(self, myc=None):
        # Get the curse out of the database
        check_curse = ("SELECT * FROM osumetatable "
                       "WHERE name = %s ")
        self.DB_CURSE.execute(check_curse, ["OsuMetaData"])
        (_, myc) = self.DB_CURSE.fetchone()
        print("Now fetching scores...")
        
        new_out, cursor_out = self.CLIENT.get_all_scores(osu.enums.GameModeStr.STANDARD, myc)

        # Print the first score processed:
        the_first_score = new_out[1][0]
        print("The first score processed here:", the_first_score.id)
        
        for score in new_out[1]:        
            details = {}
            details["map_id"] = score.beatmap_id
            details["score_id"] = score.id
            details["user_id"] = score.user_id    
            details["pp"] = score.pp
            details["time_end"] = score.ended_at.date()
            details["h"] = score.ended_at.hour

            #Check if the map id and date are already in the table
            check_score = ("SELECT * FROM osumaintable "
                           "WHERE map_id = %s AND day = %s AND h = %s "
                           "LIMIT 1")
            self.DB_CURSE.execute(check_score, (details["map_id"], details["time_end"], details["h"]))
            existing_score = self.DB_CURSE.fetchone()

            # In the table = just increment playcount by one
            # There can only be one result with a certain map id and date
            if existing_score is not None:
                (_, existing_playcount, _, _, _) = existing_score 
                update_score = ("UPDATE osumaintable "
                                "SET playcount = %s "
                                "WHERE map_id = %s AND day = %s AND h = %s")
                data_update = (existing_playcount + 1, details["map_id"], details["time_end"], details["h"])
                self.DB_CURSE.execute(update_score, data_update)
            # Add the score into the table
            else:
                add_score = ("INSERT INTO osumaintable "
                             "(map_id, playcount, day, mapset_id, h) "
                             "VALUES (%s, %s, %s, %s, %s)")
                data_score = (details["map_id"], 1, details["time_end"], None, details["h"])
                self.DB_CURSE.execute(add_score, data_score)
            self.DB_CNX.commit()

        # Update the curse (NOT the database, the scores)
        set_curse = ("UPDATE osumetatable "
                     "SET curse = %s "
                     "WHERE name = %s ")
        data_curse = (cursor_out[1], "OsuMetaData")
        self.DB_CURSE.execute(set_curse, data_curse)
        self.DB_CNX.commit()

        # Print the last score processed
        last_score = new_out[1][len(new_out[1])-1]
        print("The last score processed here:", last_score.ended_at, last_score.id)

        # Purge old scores
        old_date = last_score.ended_at.date() - timedelta(days=7)
        old_h = last_score.ended_at.hour
        old_purge = ("DELETE FROM osumaintable "
                     "WHERE day <= %s AND h <= %s")
        data_purge = (old_date, old_h)
        self.DB_CURSE.execute(old_purge, data_purge)
        self.DB_CNX.commit()

        # Also purge old maptable entries
        oldmap_purge = ("DELETE FROM osumaptable "
                        "WHERE expiry <= %s")
        oldmap_data = [datetime.now(timezone.utc).date()]
        self.DB_CURSE.execute(oldmap_purge, oldmap_data)
        self.DB_CNX.commit()

        # Debug print the size of our mapset table?
        debug_size = ("SELECT COUNT(*) FROM osumaptable ")
        self.DB_CURSE.execute(debug_size)
        print(self.DB_CURSE.fetchall())
              
    def get_top_rows(self, limit):
        # New connection which goes parallel with the old one and hopefully does not mess everything up
        my_getting_connection = mysql.connector.connect(host=self.DB_HOST, port=self.DB_PORT, username=self.DB_USERNAME, password=self.DB_PASSWORD, database=self.DB_NAME)
        my_getting_curse = my_getting_connection.cursor()
        try:
            grouped_by_mapset = ("CREATE OR UPDATE VIEW groupedandlimitedbymapset AS "
                                 "SELECT SUM(playcount) AS playcount, mapset_id FROM osumaintable "
                                 "WHERE mapset_id IS NOT NULL "
                                 "GROUP BY mapset_id "
                                 "ORDER BY playcount DESC "
                                 "LIMIT %s")
            my_getting_curse.execute(grouped_by_mapset, [limit])
            print("View of grouped by mapset created.")
        except Exception as e:
            print("Can't update the grouped_by_mapset view")
            print(e)

        top_maps = ("SELECT groupedandlimitedbymapset.playcount,osumaptable.mapset_id,osumaptable.name FROM groupedandlimitedbymapset "
                    "INNER JOIN osumaptable ON groupedandlimitedbymapset.mapset_id=osumaptable.mapset_id "
                    "ORDER BY groupedandlimitedbymapset.playcount DESC "
                    "LIMIT %s")
        my_getting_curse.execute(top_maps, [limit])
        my_maps = my_getting_curse.fetchall()
        
        my_getting_curse.close()
        my_getting_connection.close()
        return my_maps
 
    def start(self):
        self.active = True
        self.my_thread = threading.Thread(target=self._mainloop)
        self.my_thread.start()

    def _mainloop(self):
        while self.active:
            self._process_recent_scores()
            self._get_beatmap_info(60)
            time.sleep(1)
        self.DB_CURSE.close()
        self.DB_CNX.close()

# Some setup shenanigans
app = Flask(__name__)
my_handle = DataHandle()

# Actually start up
print("Starting up now")
my_handle.start()
print("Not blocked.")    

@app.route('/')
def landing_page():
    top_rows = my_handle.get_top_rows(100)
    return render_template(("index.html"), maps=top_rows)

    
