#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from datetime import datetime
from pw_utils import mydb_utils
from pw_utils import act_utils
from pw_utils import score_tables_utils as stu
from pw_utils import string_utils

note = """
for testing use
"""

class Bork:
    debug = False

    def __init__(self):
        pass
    
    def go(self, conn):
        q = """
        select siss_id, test_id, test_component, test_date, data_src, score, date_loaded, last, first, middle, email, address1, city, state, postal, country, birthdate, homephone, cphone, ophone,status, created, last_update
        from act_stage_fake
        order by siss_id
        """
        ts = mydb_utils.get_file_ts()
        out_fname = f"DU_UGRD_PW_1501_ACTFAK-{ts}.csv"
        # make array out of white space delimited string
        hdr = string_utils.pct_w("siss_id test_id test_component test_date data_src score date_loaded last first middle email address1 city state postal country birthdate homephone cphone ophone status created last_update emplid")
        with open(out_fname, "w") as fout:
            mydb_utils.uga_out(fout, hdr)
            cur = conn.cursor()
            for row in cur.execute(q):
                siss_id, test_id, test_component, test_date, data_src, score, date_loaded, last, first, middle, email, address1, city, state, postal, country, birthdate, homephone, cphone, ophone, status, created, last_update = row
                #print(f"siss_id {siss_id}")

                mydb_utils.uga_out(fout, [siss_id, test_id, test_component, test_date, data_src, score, date_loaded, last, first, middle, email, address1, city, state, postal, country, birthdate, homephone, cphone, ophone, status, created, last_update, ""])


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
        b.go(conn)
        
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass

main()

