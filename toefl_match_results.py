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
from pw_utils import toefl_utils
from pw_utils import toefl_match_result


class Bork:
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        pass
    def do_query(self, conn, filename_noext, categ1, categ2, skip_empty = False):
        hdr = toefl_match_result.ToeflMatchResult.header()
        
        count_q = """
        select count(*) from toefl_match
        where (categ = ? or categ = ?)
        """
        cur = conn.cursor()
        cnt = 0
        for row in cur.execute(count_q, (categ1, categ2)):
            cnt, = row
        print(f"{categ1} {categ2} count {cnt}")

        #if not skip_empty and cnt == 0:
        #    empty_name = f"{filename_noext}_none.txt"
        #    fout = open(empty_name, "w", encoding="UTF-8")
        #    fout.write("\n")
        #    fout.close
        #    return cnt

        csv_name = f"{filename_noext}.csv"
        with open(csv_name, "w", encoding="UTF-8") as fout:
            mydb_utils.uga_out(fout, hdr)
        
            query = """
            select toefl_record_id, scc_temp_id, emplid, ap_last, susp_last,
            ap_first, ap_pref, susp_first, ap_mid, smi, ax, sx, ap_dob, susp_dob, ap_ma1,
            ap_ha1, ap_sa1, susp_addr1, susp_addr2, susp_addr3, susp_addr4, ap_mcity,
            ap_hcity, ap_scity, susp_city, ap_mstate, ap_hstate, ap_sstate, susp_state,
            ap_mpostal, ap_hpostal, susp_postal, ap_mco, ap_hco, ap_sco, susp_co, email,
            moemail, faemail, susp_email, rat, ae, adr, dob, pos, fn, em, pem, npl, ano, ln,
            tdate, ttype, ibt_list, ibt_read, ibt_spea, ibt_writ, ibt_tot, pb_sec1, pb_sec2,
            pb_sec3, pb_conv_twe, pb_total, rpdt_list, rpdt_read, rpdt_writ, ran, filename, line
            from toefl_match 
            where (categ = ? or categ = ?)
            order by dob desc, ae desc, fn desc, rat desc 
            """
            cur = conn.cursor()
            for row in cur.execute(query, (categ1, categ2)):
                mydb_utils.uga_out(fout, row)
        
        # you have to use with to close the file. the exclude file was lacking a header row
        # sleep 2 just in case
        print("creating excel file ...")
        
        time.sleep(3)
        mydb_utils.csv_to_xlsx(csv_name)
        #cmd = os.path.join(self.script_dir, f"csv_to_xlsx.py standard {csv_name}")
        #ret = os.system(cmd)

        
    #----------------------------------
    def go(self,conn):
        skip_empty = True
        if len(sys.argv[1:]) != 1:
            print("use toefl_match_results.py 1 | 2 | both")
            sys.exit(-1)

        which_match = sys.argv[1]

        if which_match in ["1","both"]:
            self.do_query(conn, "toefl_unmatched1", "u1", "u1")

        if which_match in ["2","both"]:
            self.do_query(conn, "toefl_unmatched2", "u2", "u2")

        self.do_query(conn, "toefl_ready", "r1", "r2",skip_empty)

        # force make an empty excl file
        self.do_query(conn, "toefl_excl", "", "",skip_empty)

        
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

