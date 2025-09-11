#!/usr/bin/env python3

# Copyright Sep 2025, Philip Wright. All rights reserved. 

import random
import sys
import traceback
import sqlite3
import os.path
import time
from datetime import datetime
from pw_utils import mydb_utils


class Bork:
    debug = False
    
    def __init__(self):
        self.scc_temp_id_fake = 9000001
        pass

    def get_fake_id(self):
        fake_id = self.scc_temp_id_fake
        self.scc_temp_id_fake += 1
        return fake_id
    
    def go(self, conn):
        q = """
        select r.ran, r.last, r.first, r.middle, upper(r.email), r.a1, r.city, r.state, r.postal, r.country, r.dob_ex
        from toefl_record r, toefl_matched_keys k
        where k.toefl_record_id = r.toefl_record_id
        and k.match_date > '2024-10-31'
        and k.match_date < '2025-02-07'

        union

        select r2.ran, r2.last, r2.first, r2.middle, upper(r2.email), r2.a1, r2.city, r2.state, r2.postal, r2.country, r2.dob_ex
        from toefl_record r2, toefl_exclude k2
        where k2.toefl_record_id = r2.toefl_record_id
        and k2.excl_date > '2024-10-31'
        and k2.excl_date < '2025-02-07'
        """
        rnd = random.randint(50000001, 50300000)
        outname = f"DU_UGRD_PW_1509_TOEFAK-{rnd}.csv"
        hdr = ["ID","RAN","Last","First Name","Middle","Email","Address 1","City","State","Postal","Country","Birthdate","Status","Last Upd DtTm","emplid"]
        with open(outname, "w") as fout:
            mydb_utils.uga_out(fout, hdr)
            cur = conn.cursor()
            for row in cur.execute(q):
                ran, last, first, middle, email, a1, city, state, postal, country, dob = row
                
                vdob = mydb_utils.iso_to_date(dob)
                id = self.get_fake_id()
                
                mydb_utils.uga_out(sys.stdout, [id,ran, last, first, middle, email, a1, city, state, postal, country, vdob])

                mydb_utils.uga_out(fout, [id,ran, last, first, middle, email, a1, city, state, postal, country, vdob,"","",""])
        
        pass # def go
    pass # class Bork
        
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

