#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved.

import sys
import traceback
import sqlite3
import os.path
import time
from pw_utils import mydb_utils
from pw_utils import string_utils

note = """
  reads from toefl_ready.csv
  reformats it to toefl_ready.csv_plain.csv
  looks up the toefl record in the local database
  looks up the applicant record in applicant_biod
  any issues get written to toefl_mismatch.xlsx
"""

class Bork:
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        self.dup_emplid_hash = {}
        self.py = mydb_utils.python_name()
        

    #----------------------------------
    def lu_applicant_biod(self, conn, emplid):
        q = """
        select last, first, middle, birthdate, sex, prefname
        from applicant_biod where emplid = ? limit 1
        """
        cur = conn.cursor()
        last, first, middle, birthdate, sex, prefname = ["","","","","",""]
        for row in cur.execute(q, (emplid,)):
            last, first, middle, birthdate, sex, prefname = row
        cur.close
        return [last, first, middle, birthdate, sex, prefname]

    #----------------------------------
    def lu_toefl(self, conn, toeflrecord_id):
        if toeflrecord_id == "":
            return ["","","","",""]
        toefl_lu_q = """
        select last, first, mi, dob, sex
        from toefl_record where toefl_record_id = ?
        """
        last, first, mi, dob, sex = ["","","","",""]
        cur = conn.cursor()
        for row in cur.execute(toefl_lu_q, (toeflrecord_id,)):
            last, first, mi, dob, sex = row
        cur.close

        vdob = ""
        if len(dob) > 0:
            vdob = mydb_utils.iso_to_date(dob)
        
        return [last, first, mi, vdob, sex]
        
    #----------------------------------
    def read_file(self,conn):
        if not os.path.exists("toefl_ready.csv"):
            raise Exception("missing file toefl_ready.csv")

        if not os.path.exists("toefl_excl.csv"):
            raise Exception("missing file toefl_excl.csv")

        cmd = f"{self.py} " + os.path.join(self.script_dir, "excel_csv_to_plain_csv.py toefl_ready.csv")
        os.system(cmd)

        cmd = f"{self.py} " + os.path.join(self.script_dir, "excel_csv_to_plain_csv.py toefl_excl.csv")
        os.system(cmd)

        toeflrecord_id_hash = {}
        out_csv = "toefl_mismatch.csv"
        hdr = string_utils.pct_w("COUNT_ TOEFL_RECORD_ID_ SCC_TEMP_ID_ EMPLID_ BLN_ BMI_ BFN_ BDOB_ SLAST_ ALAST_ SFIRST_ AFIRST_ APREF_ SMI_ AMIDDLE_ SDOB_ ADOB_")
        with open(out_csv, "w") as fout:
            mydb_utils.uga_out(fout,hdr)
            # read toefl_ready.csv_plain.csv
            with open("toefl_ready.csv_plain.csv", "r") as fin:
                readfile_done = False
                read_expected_fields = 69
                read_linecount = 0
                while not readfile_done:
                    line = fin.readline()
                    if len(line) == 0:
                        readfile_done = True
                        continue
                    # skip header row
                    if read_linecount == 0:
                        read_linecount += 1
                        continue
                    vline = line.strip()
                    fields = mydb_utils.split_sq_csv_line(vline)
                    nfields = len(fields)
                    if nfields != read_expected_fields:
                        raise RuntimeError(f"line {read_linecount+1} fields {nfields} not expected {read_expected_fields}")
                    ahid, trec, emplid, ap_last, susp_last, ap_first, ap_pref, susp_first, ap_mid, smi, ax, sx, ap_dob, susp_dob, ap_ma1, ap_ha1, ap_sa1, susp_addr, susp_addr2, susp_addr3, susp_addr4, ap_mcity, ap_hcity, ap_scity, susp_city, ap_mstate, ap_hstate, ap_sstate, susp_state, ap_mpostal, ap_hpostal, susp_postal, ap_mco, ap_hco, ap_sco, susp_co, email, moemail, faemail, susp_email, rat, ae, adr, dob, pos, fn, em, npl, ano, ln, tdate, ttype, ibt_list, ibt_read, ibt_spea, ibt_writ, ibt_tot, pb_sec1, pb_sec2, pb_sec3, pb_conv_twe, pb_total, rpdt_list, rpdt_read, rpdt_writ, ran, filename, line = fields
                    if not toeflrecord_id_hash.get(ahid, None) is None:
                        raise RuntimeError(f"dup use of toeflreacord_id: {ahid} emplid: {emplid} prev emplid {emplid} lineno: {read_linecount+1}")
                    else:
                        toeflrecord_id_hash[ahid] = emplid
                    alast, afirst, amiddle, adob, asex, aprefname = self.lu_applicant_biod(conn, emplid)
                    slast,sfirst,smi,sdob,ssex = self.lu_toefl(conn, ahid)
                    
                    
                    ami = ""
                    if len(amiddle) > 0:
                        ami  = amiddle[0:1].upper()
                    last_mismatch = 0
                    first_mismatch = 0
                    middle_mismatch = 0
                    dob_mismatch = 0
                    mismatch = False
          
                    if alast.upper() != slast.upper():
                        last_mismatch = 1
                        mismatch = True
                        alast = self.flag(alast)
                        slast = self.flag(slast)
                    if afirst.upper() != sfirst.upper() and aprefname.upper() != sfirst.upper():
                        first_mismatch = 1
                        mismatch = True
                        afirst = self.flag(afirst)
                        sfirst = self.flag(sfirst)
                        aprefname = self.flag(aprefname)
                    if len(ami) == 1 and len(smi) == 1 and ami.upper() != smi.upper():
                        middle_mismatch = 1
                        mismatch = True
                        ami = self.flag(ami)
                        smi = self.flag(smi)
                    if adob != sdob:
                        dob_mismatch = 1
                        mismatch = True
                        adob = self.flag(adob)
                        sdob = self.flag(sdob)
          
                    if not self.dup_emplid_hash.get(emplid, None) is None:
                        mismatch = True
                        emplid = self.flag(emplid)

                    if mismatch:
                        mydb_utils.uga_out(fout,[read_linecount+2,ahid,trec,emplid,last_mismatch, middle_mismatch, first_mismatch, dob_mismatch,slast,alast.upper(),sfirst,afirst.upper(),aprefname.upper(), smi,ami.upper(),sdob,adob])

                    if ((read_linecount % 100) == 0):
                        print(f"{read_linecount}")
                    read_linecount += 1
                    #pass while
                # pass with
            #pass with
        # pass def
        time.sleep(2)
        mydb_utils.csv_to_xlsx(out_csv)

    #----------------------------------
    def flag(self,val):
        return f"<{val}>"
    
        
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
        b.read_file(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
        
    finally:
        if not conn is None:
            conn.close()
        pass
main()

