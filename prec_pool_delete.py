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

    def delete(self, conn, year):
        q = """
        delete from prec_pool where year = ?
        """
        cur = conn.cursor()
        cur.execute(q, (year,))
        conn.commit()
        return
        
    def go(self, conn):
        if len(sys.argv[1:]) != 1:
            print("use prec_pool_delete.py year")
            sys.exit(-1)

        year = sys.argv[1]
        self.delete(conn, year)
            
        pass # def go
    pass # class Bork
        
def main():
    conn = None
    try:
        pools_db_name = "pools.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        pools_db_file = os.path.join(db_dir, pools_db_name)
        if not os.path.exists(pools_db_file):
            raise RuntimeError(f"pools db file {scores_db_file} not found")

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

