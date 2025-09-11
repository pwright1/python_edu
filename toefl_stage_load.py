#!/usr/bin/env python3

# Copyright Sep 2025, Philip Wright. All rights reserved.

import sys
import hashlib
import traceback
import sqlite3
import os.path
import time
import re
from datetime import date
from pw_utils import mydb_utils
from pw_utils import score_tables_utils
from pw_utils import toefl_utils

note = """
"""

class Bork:
    debug = False

    def delete(self, conn):
        delete_q = "delete from toefl_stage"
     
    def __init__(self):
        pass

    def read_ps_toefl_stage_query_file(self, fout, conn):
        
        dict = {}
        now = date.today()
        expected_fields = 15
        if len(sys.argv[1:]) == 0:
            print("use _ filename.csv")
            sys.exit()
        fname = sys.argv[1]

        if len(fname) < 27:
            raise RuntimeError ("File name needs to have _1509_TOE in it to load 1")

        match_arr = re.findall(r"_1509_TOE",fname)
        if len(match_arr) != 1 or match_arr[0] != "_1509_TOE":
            raise RuntimeError ("File name needs to have _1509_TOE in it to load 2")
        
        if self.debug:
            print("the filename is {}".format(fname))
        with open(fname, "r", encoding="UTF-8") as file:
            line_count = 0
            last_scc_temp_id = ""
            done = False
            while not done:
                line = file.readline()
                if len(line) == 0:
                    done = True
                    continue
                # skip header line
                if line_count == 0:
                    line_count += 1
                    continue
                skip_insert = False
                if self.debug:
                    print("while loop")
                vline = line.strip()
                fields = mydb_utils.split_sq_csv_line(vline)
                nfields = len(fields)
                if nfields != expected_fields:
                    print("field count {} not expected {}".format(nfields, expected_fields))
                    sys.exit(1)
                scc_temp_id, ran, last, first, middle, email, addr1, city, state, postal, country, dob, status, lastupd, emplid = fields
                vdob = mydb_utils.date_to_iso(dob)
                vlast = last.upper()[0:30]
                vfirst = first.upper()[0:30]
                vmi = middle.upper()[0:1]
                vemail = email.upper()[0:50]
                vaddr1 = addr1.upper()[0:40]
                vcity = city.upper()[0:30]

                # ps adds them when they are blank
                if vfirst == "MR" or vfirst == "MS" or vfirst == ".":
                    vfirst = ""
                
                # remove leading zeros
                vran = list(ran)
                for item in list(ran):
                    if item == "0":
                        vran.pop(0)
                    else:
                        break
                vran = "".join(vran)

                digparts, digest_nld = toefl_utils.calc_digest_nld(vran, vlast, vfirst, vmi, vdob, vemail, vcity, country)

                tup = (scc_temp_id, vran, last, first, middle, email, addr1, city, state, postal, country, dob, status, lastupd, emplid, digparts, digest_nld)
                
                self.insert(conn, tup)
                
                if line_count % 1000 == 0:
                    print(line_count)
                    conn.commit()
                line_count += 1
            conn.commit()
            pass # while
        pass # with

    def insert(self, conn, tup):
        insert_q = """
        insert into toefl_stage (scc_temp_id, ran, last, first, middle, email, addr1, city, state, postal, country, dob, status, last_upd, emplid, digparts, digest_nld) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  on conflict do nothing
        """
        cur = conn.cursor()
        cur.execute(insert_q, tup)

    def update_stage_record(self, conn, tup):
        q = "update toefl_stage set toefl_record_id = ? where scc_temp_id = ?"
        cur = conn.cursor()
        res = cur.execute(q, (tup))
        
    def update_toefl_record_id(self, conn):
        select_q = """
        select s.scc_temp_id, r.toefl_record_id
        from toefl_stage s, toefl_record r
        where s.digest_nld = r.digest_nld
        """
        update_arr = []
        cur = conn.cursor()
        for row in cur.execute(select_q):
            scc_temp_id, toefl_record_id = row
            #mydb_utils.uga_out(sys.stdout, [scc_temp_id, toefl_record_id])
            update_arr.append((toefl_record_id,scc_temp_id))
        for i, tup in enumerate(update_arr):
            self.update_stage_record(conn,tup)
            if i % 100 == 0:
                conn.commit()
                print(i)
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
        score_tables_utils.toefl_stage_table_drop(conn)
        score_tables_utils.toefl_stage_table_create(conn)
        b = Bork()
        b.debug = False
        fout = None
        b.read_ps_toefl_stage_query_file(fout, conn)
        b.update_toefl_record_id(conn)
        #fout.close
    except Exception as err:
        print(f"Exception {err} {type(err)}")
        traceback.print_exc()
        
    finally:
        if not conn is None:
            conn.close()
        pass
main()

