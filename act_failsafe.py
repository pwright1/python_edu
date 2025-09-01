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
  reads from act_ready.csv
  reformats it to act_ready.csv_plain.csv
  looks up the act record in the local database
  looks up the applicante record in applicant_biod
  any issues get written to act_mismatch.xlsx
"""

class Bork:
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        self.dup_emplid_hash = {}
        self.py = mydb_utils.python_name()
        
    #----------------------------------
    def tally_applicant_dup_emplids(self, conn):
        print("tally start")
        emplid_arr = []
        dup_q = """
        select distinct a.emplid
        from applicant_biod a,
        applicant_biod b
        where 1 = 1
        and (
        (upper(a.last) = upper(b.last) and upper(a.first) = upper(b.first))
        or
        a.email = b.email
        )
        and a.birthdate = b.birthdate
        and a.appno <> b.appno
        """
        cur = conn.cursor()
        for row in cur.execute(dup_q):
            emplid, = row
            emplid_arr.append(emplid)
        cur.close
        self.dup_emplid_hash[emplid] = True
        print("tally done")

    #----------------------------------
    def lu_applicant_biod(self,conn, emplid):
        #print("biod_lu")
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
    def lu_act(self, conn, actrecord_id):
        #print("act_lu")
        if actrecord_id == "":
            return ["","","","",""]
        act_lu_q = """
        select last, first, mi, edob, genderalpha
        from actrecord where actrecord_id = ?
        """
        alast, acidst, ami, adob, agender = ["","","","",""]
        cur = conn.cursor()
        for row in cur.execute(act_lu_q, (actrecord_id,)):
            alast, afirst, ami, adob, agender = row
        cur.close
        
        vadob = ""
        if len(adob) > 0:
            vadob = mydb_utils.iso_to_date(adob)
        
        return [alast, afirst, ami, vadob, agender]
        
    #----------------------------------
    def read_file(self,conn):
        if not os.path.exists("act_ready.csv"):
            raise Exception("missing file act_ready.csv")

        if not os.path.exists("act_excl.csv"):
            raise Exception("missing file act_excl.csv")

        cmd = f"{self.py} " + os.path.join(self.script_dir, "excel_csv_to_plain_csv.py act_ready.csv")
        os.system(cmd)

        cmd = f"{self.py} " + os.path.join(self.script_dir, "excel_csv_to_plain_csv.py act_excl.csv")
        os.system(cmd)

        actrecord_id_hash = {}
        out_csv = "act_mismatch.csv"
        hdr = string_utils.pct_w("COUNT_ ACTRECORD_ID_ SCC_TEMP_ID_ EMPLID_ BLN_ BMI_ BFN_ BDOB_ SLAST_ ALAST_ SFIRST_ AFIRST_ APREF_ SMI_ AMIDDLE_ SDOB_ ADOB_")
        with open(out_csv, "w") as fout:
            mydb_utils.uga_out(fout,hdr)
            # read act_ready.csv_plain.csv
            with open("act_ready.csv_plain.csv", "r") as fin:
                readfile_done = False
                read_expected_fields = 59
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
                    ahid,trec,emplid,ldate,actid,ap_last,susp_last,ap_first,ap_pref,susp_first,ap_mid,smi,ax,sx,ap_dob,susp_dob,ap_ma1,ap_ha1,ap_sa1,susp_addr,ap_mcity,ap_hcity,ap_scity,susp_city,ap_mstate,ap_hstate,ap_sstate,susp_state,ap_mpostal,ap_hpostal,susp_postal,ap_mco,ap_hco,ap_sco,susp_co,hphone,cphone,mophone,faphone,susp_phone,email,moemail,faemail,susp_email,ap_atp,susp_atp,rat,ape,adr,dob,pho,atp,pos,fn,em,npl,ano,ln,fdup = fields
                    if not actrecord_id_hash.get(ahid, None) is None:
                        raise RuntimeError(f"dup use of actreacord_id: {ahid} emplid: {emplid} prev emplid {emplid} lineno: {read_linecount+1}")
                    else:
                        actrecord_id_hash[ahid] = emplid
                    alast, afirst, amiddle, adob, asex, aprefname = self.lu_applicant_biod(conn, emplid)
                    slast,sfirst,smi,sdob,ssex = self.lu_act(conn, ahid)
                    
                    
                    #mydb_utils.uga_out(sys.stdout, [ahid, emplid,alast, slast, afirst, sfirst])

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
                        mydb_utils.uga_out(fout,[read_linecount+2,ahid,trec,emplid,last_mismatch, middle_mismatch, first_mismatch, dob_mismatch,slast,alast.upper(),sfirst,afirst.upper(),aprefname.upper(),smi,ami.upper(),sdob,adob])

                    if ((read_linecount % 10) == 0):
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
        # too slow...
        #b.tally_applicant_dup_emplids(conn)
        b.read_file(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
        
    finally:
        if not conn is None:
            conn.close()
        pass
main()

