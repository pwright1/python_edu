#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import csv
from pw_utils import mydb_utils
from pw_utils import string_utils

note = """
loads slate ps export query data file with 646 fields with language data
loads old file with 619 fields and leave lang fields blank
this is so you can fix errors in the file in a database editor
then recreate the generated file with app_ps_query.py
"""

class Bork:
    debug = False
    
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        pass

    def delete(self, conn):
        q1 = "delete from app_ps_lang"
        q2 = "delete from app_ps_work"
        q3 = "delete from app_ps_edu"
        q4 = "delete from app_ps_rel"
        q5 = "delete from app_ps_main"
        cur = conn.cursor()

        print("this will delete data for the current appload you have loaded")
        print("are you sure? (type YES)")
        answer = input()
        if answer == "YES":
            for q in [q1,q2,q3,q4,q5]:
                cur.execute(q)
            conn.commit()
        
    pass # class
        
def main():
    conn = None
    try:
        apps_db_name = "apps.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        apps_db_file = os.path.join(db_dir, apps_db_name)
        if not os.path.exists(apps_db_file):
            raise RuntimeError(f"apps db file {apps_db_file} not found")

        conn = mydb_utils.sqlite3_connect(apps_db_file)
        b = Bork()
        #b.debug = True
        b.delete(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()
