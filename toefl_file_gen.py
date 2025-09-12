#!/usr/bin/env python3

# Copyright Sep 2025, Philip Wright. All rights reserved.

import sys
import traceback
import sqlite3
import os.path
import time
from pw_utils import mydb_utils
#from pw_utils import string_utils

note = """
generates a matched toefl upload data file from the local database
input the match date as YYYY-MM-DD.
you can query toefl_matched_keys by match_date desc to see the date you need
"""

class Bork:
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        self.country_hash = {}
        
    def load_country(self, conn):
        q = "select country, country3 from slate_country"
        cur = conn.cursor()
        for row in cur.execute(q):
            country, country3 = row
            if self.country_hash.get(country, None) == None:
                self.country_hash[country] = country3
        cur.close()

    def go(self, conn):

        if len(sys.argv) != 2:
            print("use toefl_file_gen.py yyyy-mm-dd (for match date)")
            sys.exit(-1)

        match_date = sys.argv[1]

        hdr = ["Emplid","Ran","Last","First","Middle","Date of Birth","Gender","Email","Address1","Address2","Address3","Address4","City","State","Country3","Country","Postal","Test Date","Test Type","IBT Listening","IBT Reading","IBT Speaking","IBT Writing","IBT Total Score", "Filename", "Line Number"]
        ts = mydb_utils.get_utc_ts()
        fname = f"matched_toefl_{ts}.csv"

        with open(fname, "w", encoding="UTF-8") as fout:
            mydb_utils.uga_out(fout, hdr)
            q = """
            select
            k.emplid,
            r.ran,
            r.last,
            r.first,
            r.middle,
            r.dob,
            r.gender_ex,
            r.email,
            r.a1,
            r.a2,
            r.a3,
            r.a4,
            r.city,
            r.state,
            r.country,
            r.countryde,
            r.postal,
            r.admin_date,
            r.test_type,
            r.ibt_listening,
            r.ibt_reading,
            r.ibt_speaking,
            r.ibt_writing,
            r.ibt_total,
            f.filename,
            r.fileline
            from toefl_file f, toefl_record r, toefl_matched_keys k
            where r.toefl_file_id = f.toefl_file_id
            and k.toefl_record_id = r.toefl_record_id
            and substring(k.match_date,1,10) = ?
            and r.test_type = 'I'
            order by r.last, r.first, r.middle, r.dob
            
            """
            cur = conn.cursor()
            count = 0
            for row in cur.execute(q, (match_date,)):
                emplid, ran, last, first, middle, dob, sex, email, a1, a2, a3, a4, city, state, country, countryde, postal, admin_date, test_type, ibt_listening, ibt_reading, ibt_speaking, ibt_writing, ibt_total, filename, fileline = row
                
                vcountryde = self.country_hash.get(country, countryde)
            
                mydb_utils.uga_out(fout, [emplid, ran, last, first, middle, dob, sex, email, a1, a2, a3, a4, city, state, country, vcountryde, postal, admin_date, test_type, ibt_listening, ibt_reading, ibt_speaking, ibt_writing, ibt_total, filename, fileline])
                count += 1
            print(f"row count {count}")
        

def main():
    conn = None
    try:
        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        scores_db_file = os.path.join(db_dir, scores_db_name)
        
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")
        #print(f"scores db file {scores_db_file} not found. will be created")
        
        conn = mydb_utils.sqlite3_connect(scores_db_file)
        b = Bork()
        b.load_country(conn)
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
        
    finally:
        if not conn is None:
            conn.close()
        pass
main()

