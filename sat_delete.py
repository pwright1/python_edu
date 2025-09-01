#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import os
import traceback
import sqlite3
from pw_utils import mydb_utils
from pw_utils import string_utils

class Bork:
    debug = False
    def __init__(self):
        pass

    def do_delete(self, conn, satrecord_id):
        #print(f"delete using satrecord_id {satrecord_id}")
        q4 = "delete from sat_exclude where satrecord_id = ?"
        q3 = "delete from sat_matched_keys where satrecord_id = ?"
        q2 = "delete from satrecord_addl_2 where satrecord_id = ?"
        q1 = "delete from satrecord_addl_1 where satrecord_id = ?"
        q0 = "delete from satrecord where satrecord_id = ?"

        for q in [q4,q3,q2,q1,q0]:
            cur = conn.cursor()
            res = cur.execute(q,(satrecord_id,))
            #print(f"delete row count {cur.rowcount}")
            conn.commit()
        
    def go(self, conn):
        if len(sys.argv[1:]) == 0:
            print("use python sat_delete.py satrecord_id1 satrecord_id2 ...(space separated list) ")
            sys.exit()
        for satrecord_id in sys.argv[1:]:
            type, int_val = string_utils.mytype(satrecord_id)
            if type != "int":
                printf(f"non integer id: {satrecord_id} ignored")
                continue
            #print(f"would delete id {int_val}")
            self.do_delete(conn, int_val)
            #self.do_delete(conn, satrecord_id)
            
    pass # class Bork

def main():
    conn = None
    try:
        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        #print(f"using db dir {db_dir}")
        scores_db_file = os.path.join(db_dir, scores_db_name)
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")
        conn = mydb_utils.sqlite3_connect(scores_db_file)
        b = Bork()
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
main()

