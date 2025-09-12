#!/usr/bin/env python3

# Copyright Sep 2025, Philip Wright. All rights reserved.

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
        
    def keys_insert(self, conn, toefl_record_id, emplid, match_date, trec):
        q = """
        insert into toefl_matched_keys (toefl_record_id, emplid, match_date, scc_temp_id) 
        values (?,?,?,?) on conflict do nothing
        """
        cur = conn.cursor()
        cur.execute(q, (toefl_record_id, emplid, match_date, trec))
        
    def go(self, conn):
        if not (len(sys.argv[1:]) == 1 and sys.argv[1] == "toefl_ready.csv_plain.csv"):
            raise RuntimeError("use: python toefl_susp_update.py toefl_ready.csv_plain.csv")
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        inname = sys.argv[1]
        outname = "toefl_stage.txt"
        expected_fields = 69
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
                    if len(fields) != expected_fields:
                        print(f"unexpected field count {len(fields)} not expected value {expected_fields}")
                    
                    ahid, trec, emplid, ap_last, susp_last, ap_first, ap_pref, susp_first, ap_mid, smi, ax, sx, ap_dob, susp_dob, ap_ma1, ap_ha1, ap_sa1, susp_addr, susp_addr2, susp_addr3, susp_addr4, ap_mcity, ap_hcity, ap_scity, susp_city, ap_mstate, ap_hstate, ap_sstate, susp_state, ap_mpostal, ap_hpostal, susp_postal, ap_mco, ap_hco, ap_sco, susp_co, email, moemail, faemail, susp_email, rat, ae, adr, dob, pos, fn, em, pem, npl, ano, ln, tdate, ttype, ibt_list, ibt_read, ibt_spea, ibt_writ, ibt_tot, pb_sec1, pb_sec2, pb_sec3, pb_conv_twe, pb_total, rpdt_list, rpdt_read, rpdt_writ, ran, filename, line = fields
                    
                    if ahid == "" or emplid == "" or ts == "" or trec == "":
                        mydb_utils.uga_out(sys.stdout,
                                           ["keys ins err:",ahid, emplid, ts, trec])
                        raise RuntimeError("blank keys insert value")

                    self.keys_insert(conn, ahid, emplid, ts, trec)
                    mydb_utils.uga_out_noquote(fout, ["DU_TOEFL_TEST_TRANSACTION", trec, emplid])
                    #if line_count % 10 == 0:
                    #    conn.commit()
                    line_count += 1
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

