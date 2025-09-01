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
"""

class Bork:
    debug = False
    
    #--------------------------------------
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        pass

    #--------------------------------------
    def update_last(self, conn, name_last, email, id):
        update_q = """
        update app_ps_main set name_last = ?, email_address_home = ? where id = ?
        """
        cur = conn.cursor()
        res = cur.execute(update_q, (name_last, email, id))

    #--------------------------------------
    def go(self, conn):
        if len(sys.argv[1:]) != 1:
            print("use app_ps_mangle_last.py 3char_str")
            sys.exit(-1)

        last_prefix_str = sys.argv[1]

        # need to do ssn, phone
        select_q = """
        select id, name_last,email_address_home
        from app_ps_main
        """
        update_arr = []
        cur = conn.cursor()
        for row in cur.execute(select_q):
            id, name_last, email = row
            update_str = f"{id}^{last_prefix_str.upper()}{name_last}^{last_prefix_str.upper()}{email}"
            update_arr.append(update_str)
        for i, update_str in enumerate(update_arr):
            id, name_last, email = update_str.split("^")
            self.update_last(conn, name_last, email, id)
            if i % 20 == 0:
                conn.commit()
        conn.commit()
        pass # def go
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
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()
