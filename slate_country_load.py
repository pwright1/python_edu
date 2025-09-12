#!/usr/bin/env python3

# Copyright Sep 2025, Philip Wright. All rights reserved.

import sys
import traceback
import sqlite3
import os.path
import time
from pw_utils import mydb_utils
from pw_utils import string_utils
from datetime import datetime

note = """
"""

class Bork:
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        
    def country_insert(self, conn, country, country3):
        q = """
        insert into slate_country (country, country3) values (?,?)
        """
        cur = conn.cursor()
        cur.execute(q, (country, country3))
        
    def go(self, conn):
        inname = sys.argv[1]
        expected_fields = 2
        with open(inname, "r") as fin:
            line_count = 1
            done = False
            while not done:
                line = fin.readline()
                if len(line) == 0:
                    done = True
                    continue
                # skip header line
                if line_count == 1:
                    line_count += 1
                    continue
                # blank line
                if len(line) == 1:
                    line_count += 1
                    continue
                line = line.strip()
                # should do encoding check / fix here if not valid utf-8
                fields = mydb_utils.split_sq_csv_line(line)

                if len(fields) != expected_fields:
                    print(f"unexpected field count {len(fields)} not expected value {expected_fields}")
                country, country3 = fields
                    
                self.country_insert(conn, country, country3)
                if line_count % 10 == 0:
                     conn.commit()
                line_count += 1
            conn.commit()

def main():
    conn = None
    try:
        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        scores_db_file = os.path.join(db_dir, scores_db_name)
        
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")
        #print(f"scores db file {scores_db_file} not found. will be created")
        
        conn = mydb_utils.sqlite3_connect(scores_db_file)
        b = Bork()
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
        
    finally:
        if not conn is None:
            conn.close()
        pass
main()

