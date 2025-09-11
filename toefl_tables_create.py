#!/usr/bin/env python3

# Copyright Sep 2025, Philip Wright. All rights reserved. 

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
        print("this will destroy all TOEFL data and recreate all tables, are you sure??? (type YES)")
        answer = input()
        if answer.upper() == "YES":
            #stu.toefl_exclude_table_drop(conn)
            #stu.toefl_matched_keys_table_drop(conn)
            stu.toefl_match_table_drop(conn)
            #stu.toefl_stage_table_drop(conn)
            #stu.toefl_record_table_drop(conn)
            #stu.toefl_file_table_drop(conn)

            #stu.toefl_file_table_create(conn)
            #stu.toefl_record_table_create(conn)
            #stu.toefl_stage_table_create(conn)
            stu.toefl_match_table_create(conn)
            #stu.toefl_matched_keys_table_create(conn)
            #stu.toefl_exclude_table_create(conn)
            print("done")
        else:
            print("answer needst to be YES to comply")
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

