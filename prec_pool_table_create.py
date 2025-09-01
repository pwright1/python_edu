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
        q = "drop table if exists prec_pool"
        cur = conn.cursor()
        cur.execute(q)
        return

    def create(self, conn):
        q = """
        create table if not exists prec_pool(
        id integer,
        emplid text,
        last text,
        first text,
        middle text,
        pref text,
        suffix text,
        dob text,
        nid text,
        a1 text,
        a2 text,
        city text,
        state text,
        postal text,
        country text,
        phone text,
        email text,
        term text,
        year text,
        sex text,
        primary key (id asc))
        """
        cur = conn.cursor()
        cur.execute(q)
        return
        
    def go(self, conn):
        if len(sys.argv[1:]) != 0:
            print("use prec_pool_table_create.py")
            sys.exit(-1)
            
        self.destroy(conn)
        self.create(conn)

    pass # class Bork
        
def main():
    conn = None
    try:
        pools_db_name = "pools.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        pools_db_file = os.path.join(db_dir, pools_db_name)
        if not os.path.exists(pools_db_file):
            raise Exception(f"pools db file {pools_db_file} not found")

        conn = mydb_utils.sqlite3_connect(pools_db_file)
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

