#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import os
import traceback
import sqlite3
from pw_utils import mydb_utils
from pw_utils import string_utils
from pw_utils import act_utils

class Bork:
    debug = False
    def __init__(self):
        pass

    def go(self, conn):
        if len(sys.argv[1:]) == 0:
            print("use python act_delete.py actrecord_id1 actrecord_id2 ...(space separated list) ")
            sys.exit()

        actrecord_ids = []
        for actrecord_id in sys.argv[1:]:
            type, int_val = string_utils.mytype(actrecord_id)
            if type != "int":
                printf(f"non integer id: {actrecord_id} ignored")
                continue
            actrecord_ids.append(actrecord_id)

        print("Are you sure? Answer Yes")
        response = input()
        if response.lower() == "yes":
            print(f"deleting records {actrecord_ids}")
            act_utils.delete_act_records(conn, actrecord_ids)
            conn.commit()
            
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

