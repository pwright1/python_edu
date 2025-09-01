#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved.

import sys
import traceback
import sqlite3
import os.path
import time
from datetime import datetime
from pw_utils import mydb_utils
from pw_utils import score_tables_utils as stu


class Bork:
    debug = False
    
    def __init__(self):
        pass

    def go(self, conn):
        print("this will destroy all SAT data and recreate all tables empty, are you sure??? (type YES)")
        print("please don't do this...")
        answer = input()
        if answer == "YES":

            hide1 = """

            stu.sat_stage_fake_seq_table_drop(conn)
            stu.sat_stage_fake_table_drop(conn)
            stu.sat_country_lu_table_drop(conn)
            stu.sat_lu_table_drop(conn)
            stu.sat_exclude_table_drop(conn)
            stu.sat_matched_keys_table_drop(conn)
            """
            stu.sat_match_table_drop(conn)
            hide2 = """
            stu.satstage_table_drop(conn)
            stu.satrecord_addl_2_table_drop(conn)
            stu.satrecord_addl_1_table_drop(conn)
            stu.satrecord_table_drop(conn)
            stu.satfile_table_drop(conn)

            stu.satfile_table_create(conn)
            stu.satrecord_table_create(conn)
            stu.satrecord_addl_1_table_create(conn)
            stu.satrecord_addl_2_table_create(conn)
            stu.satstage_table_create(conn)
            """
            stu.sat_match_table_create(conn)
            hide3 = """
            stu.sat_matched_keys_table_create(conn)
            stu.sat_exclude_table_create(conn)
            stu.sat_lu_table_create(conn)
            stu.sat_country_lu_table_create(conn)
            stu.sat_country_lu_table_load(conn)
            stu.sat_stage_fake_seq_table_create(conn)
            stu.sat_stage_fake_table_create(conn)
            """
            conn.commit()
            pass
        pass # def go
    pass # class Bork
        
def main():
    conn = None
    try:
        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        scores_db_file = os.path.join(db_dir, scores_db_name)
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")

        conn = mydb_utils.sqlite3_connect(scores_db_file)
        b = Bork()
        #b.debug = True
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()

