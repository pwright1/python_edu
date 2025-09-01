#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from pw_utils import mydb_utils
from pw_utils import score_tables_utils as stu

note = """
  updates actfile loaddate from the oldloaddate field
"""

class Bork:
    debug = False
    
    def __init__(self):
        pass


    def update_actfile_rec(self, conn, tup):
        q = """
        update actfile set loaddate = ?
        where actfile_id = ?
        """
        cur = conn.cursor()
        res = cur.execute(q, (tup))
        
    def go(self, conn):
        lookup_results_arr = []
        q = "select actfile_id, oldloaddate from actfile"
        cur = conn.cursor()
        for row in cur.execute(q):
            actfile_id, oldloaddate = row
            #mydb_utils.uga_out(sys.stdout, [str(actfile_id), oldloaddate])
            lookup_results_arr.append(  (oldloaddate, actfile_id) )
            for i, tup in enumerate(lookup_results_arr):
                self.update_actfile_rec(conn, tup)
                if i % 100 == 0:
                    conn.commit()
            conn.commit()


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

