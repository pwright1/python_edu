#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from pw_utils import mydb_utils
from pw_utils import string_utils
from datetime import datetime

note = """
"""

class Bork:
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        
    def keys_insert(self, conn, actrecord_id, siss_load_date, emplid, match_date, trec):
        q = """
        insert into act_matched_keys (actrecord_id, siss_load_date, emplid, match_date, scc_temp_id) 
        values (?,?,?,?,?)
        """
        cur = conn.cursor()
        cur.execute(q, (actrecord_id, siss_load_date, emplid, match_date, trec))
        
    def go(self, conn):
        if not (len(sys.argv[1:]) == 1 and sys.argv[1] == "act_ready.csv_plain.csv"):
            raise RuntimeError("use: python act_susp_update.py act_ready.csv_plain.csv")
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        inname = sys.argv[1]
        outname = "act_stage.txt"
        with open(outname, "w") as fout:
            with open(inname, "r") as fin:
                line_count = 1
                done = False
                while not done:
                    line = fin.readline()
                    if len(line) == 0:
                        done = True
                        continue
                    # skip header line
                    if line_count == 1:
                        line_count += 1
                        continue
                    # blank line
                    if len(line) == 1:
                        line_count += 1
                        continue

                    line = line.strip()
                    # should do encoding check / fix here if not valid utf-8
                    fields = mydb_utils.split_sq_csv_line(line)

                    ahid,trec,emplid,ldate,actid,ap_last,susp_last,ap_first,ap_pref,susp_first,ap_mid,smi,ax,sx,ap_dob,susp_dob,ap_ma1,ap_ha1,ap_sa1,susp_addr,ap_mcity,ap_hcity,ap_scity,susp_city,ap_mstate,ap_hstate,ap_sstate,susp_state,ap_mpostal,ap_hpostal,susp_postal,ap_mco,ap_hco,ap_sco,susp_co,hphone,cphone,mophone,faphone,susp_phone,email,moemail,faemail,susp_email,ap_atp,susp_atp,rat,ape,adr,dob,pho,atp,pos,fn,em,npl,ano,ln,fdup = fields
                
                    self.keys_insert(conn, ahid, ldate, emplid, ts, trec)
                    mydb_utils.uga_out_noquote(fout, ["DU_ACT_TEST_TRANSACTION", trec, emplid])
                    if line_count % 10 == 0:
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

