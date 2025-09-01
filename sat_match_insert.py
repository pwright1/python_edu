#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved.

import sys
import traceback
import sqlite3
import os.path
import time
from datetime import date
from pw_utils import mydb_utils
from pw_utils import score_tables_utils
from pw_utils import sat_utils

"""
"""

class Bork:
    def __init__(self):
        pass

    def insert(self, conn, tup):
        insert_q = """
        insert into sat_match(
        satrecord_id, 
        categ, 
        trec, 
        emplid, 
        ldate, 
        ap_last, 
        susp_last,
        ap_first,
        ap_pref,
        susp_first,
        ap_mid,
        smi,
        ax,
        sx,
        ap_dob,
        susp_dob,
        ap_ma1,
        ap_ha1,
        ap_sa1,
        susp_addr,
        ap_mcity,
        ap_hcity,
        ap_scity,
        susp_city,
        ap_mstate,
        ap_hstate,
        ap_sstate,
        susp_state,
        ap_mpostal,
        ap_hpostal,
        susp_postal,
        ap_mco,
        ap_hco,
        ap_sco,
        susp_co,
        ap_ceeb,
        susp_ceeb,
        ap_sname,
        susp_sname,
        hphone,
        cphone,
        mophone,
        faphone,
        susp_phone,
        email,
        moemail,
        faemail,
        susp_email,
        rat,
        ape,
        adr,
        dob,
        pho,
        ceeb,
        pos,
        fn,
        em,
        npl,
        ano,
        ln
        ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        on conflict do nothing
        """
        cur = conn.cursor()
        cur.execute(insert_q, tup)

    def go(self,conn):
        if len(sys.argv[1:]) != 2:
            print("use sat_match_insert.py u1|u2|r1|r2 sat_name.txt")
            sys.exit(-1)
        category = sys.argv[1]
        if category not in ["u1","u2","r1","r2"]:
            print(f"unexpected category type {category} should be u1, u2, r1, r2")
            sys.exit(-2)
        fname = sys.argv[2]
        expected_fields = 59
        with open(fname, "r", encoding="UTF-8") as file:
            line_count = 0
            done = False
            while not done:
                line = file.readline()
                if len(line) == 0:
                    done = True
                    continue
                # skip header row
                if line_count == 0:
                    line_count += 1
                    continue
                vline = line.strip()
                fields = mydb_utils.split_sq_csv_line(vline)
                nfields = len(fields)
                if nfields != expected_fields:
                    print(f"field count {nfields} not expected {expected_fields}")
                    sys.exit(1)
                    
                ahid, trec, emplid, ldate, ap_last, susp_last, ap_first, ap_pref, susp_first, ap_mid, smi, ax, sx, ap_dob, susp_dob, ap_ma1, ap_ha1, ap_sa1, susp_addr, ap_mcity, ap_hcity, ap_scity, susp_city, ap_mstate, ap_hstate, ap_sstate, susp_state, ap_mpostal, ap_hpostal, susp_postal, ap_mco, ap_hco, ap_sco, susp_co, ap_ceeb, susp_ceeb, ap_sname, susp_sname, hphone, cphone, mophone, faphone, susp_phone, email, moemail, faemail, susp_email, rat, ape, adr, dob, pho, atp, pos, fn, em, npl, ano, ln = fields

                tup = (ahid, category, trec, emplid, ldate, ap_last, susp_last, ap_first, ap_pref, susp_first, ap_mid, smi, ax, sx, ap_dob, susp_dob, ap_ma1, ap_ha1, ap_sa1, susp_addr, ap_mcity, ap_hcity, ap_scity, susp_city, ap_mstate, ap_hstate, ap_sstate, susp_state, ap_mpostal, ap_hpostal, susp_postal, ap_mco, ap_hco, ap_sco, susp_co, ap_ceeb, susp_ceeb, ap_sname, susp_sname, hphone, cphone, mophone, faphone, susp_phone, email, moemail, faemail, susp_email, rat, ape, adr, dob, pho, atp, pos, fn, em, npl, ano, ln)
                self.insert(conn, tup)
                if line_count % 100 == 0:
                    conn.commit()
                line_count += 1
            conn.commit()
            pass # with
        pass # def go()

            
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
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
        
    finally:
        if not conn is None:
            conn.close()
        pass
main()

