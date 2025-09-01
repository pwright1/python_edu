#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
from pw_utils import mydb_utils

note = """
"""

class Bork:
    debug = False
    
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        pass

    def destroy(self, conn):
        q = "drop table if exists org"
        cur = conn.cursor()
        cur.execute(q)
        return

    def create(self, conn):
        q = """
        create table if not exists org(
        orgid text,
        orgtype text,
        atp text,
        a1 text,
        a2 text,
        a3 text,
        a4 text,
        city text,
        state text,
        postal text,
        country text,
        name text,
        estatus text,
        edate text,
        ls_sch_type text,
        org_location text,
        primary key (orgid)
        )
        """
        cur = conn.cursor()
        cur.execute(q)
        return
        
    def go(self, conn):
        if len(sys.argv[1:]) != 0:
            print("use org_table_create.py")
            sys.exit(-1)
            
        self.destroy(conn)
        self.create(conn)

    pass # class Bork
        
def main():
    conn = None
    try:
        apps_db_name = "apps.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        apps_db_file = os.path.join(db_dir, apps_db_name)
        if not os.path.exists(apps_db_file):
            raise RuntimeError(f"apps db file {scores_db_file} not found")

        conn = mydb_utils.sqlite3_connect(apps_db_file)
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

