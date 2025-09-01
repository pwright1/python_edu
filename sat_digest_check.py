#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from datetime import datetime
from pw_utils import mydb_utils
from pw_utils import sat_utils
from pw_utils import score_tables_utils as stu
from pw_utils import string_utils

note = """
one time use to fix wrong digest values due to trailing space in address1 field
"""

class Bork:
    debug = False
    
    def __init__(self):
        pass

    def do_update(self, conn, arr):
        q = "update satrecord set digparts = ?, digest_nld = ? where satrecord_id = ?"
        cur = conn.cursor()
        for i, row in enumerate(arr):
            satrecord_id, digparts, digest_nld, eoa = row.split("^")
            mydb_utils.uga_out(sys.stdout, [i, satrecord_id, digest_nld])
            cur.execute(q, (digparts, digest_nld, satrecord_id))
        conn.commit()

    def digparts_split_fix(self, digparts):
        fields = digparts.split(",")
        fields_s = [x.strip() for x in fields]
        return fields_s
        
    def go(self, conn, ts):
        update_arr = []
        digest_mismatch_count = 0
        q = """
        select r.satrecord_id, r.tdate_1, r.erws_1, r.mss_1, r.total_1, r.last, r.first, r.mi, r.dob, r.phone, r.email, r.addr1, r.city, r.state, r.postal, r.country3, r.digparts, r.digest_nld
        from satrecord r
        """
        hdr = string_utils.pct_w("satrecord_id  erws td mss td total td last first mi dob email addr1 city state postal digparts db_digest_nld digest_nld")
        cur = conn.cursor()
        i = 0
        rowcount = 0
        ofile = "sat_digest_out.csv"
        with open(ofile, "w") as fout:
            mydb_utils.uga_out(fout, hdr)
            for row in cur.execute(q):
                satrecord_id, testdate, erws, mss, total, last, first, mi, dob, phone, email, addr1, city, state, postal, country, digparts, db_digest_nld = row

                vlast = last.upper()[0:30].strip()
                vfirst = first.upper()[0:30].strip()
                vmi = mi.upper()[0:1].strip()
                vaddr1 = addr1.upper()[0:40].strip()
                vemail = email.upper()[0:50].strip()
                vcity = city.upper()[0:30].strip()

                vstate = ""
                vpostal = ""
                if country.upper() == "USA":
                    vpostal = postal[0:5].strip()
                    vstate = state[0:2].strip()
        
                # data stored in db has testdate as YYYYMM
                tdate_ym = f"{testdate[0:4]}{testdate[5:7]}"
            
                digest_nld  = sat_utils.calc_digest_nld(erws, tdate_ym, mss, tdate_ym, total, tdate_ym, vlast, vfirst, vmi, dob, vemail, vaddr1, vcity, vstate, vpostal)

                if db_digest_nld != digest_nld:
                    digest_mismatch_count += 1
                    print(f"digest mismatch for {satrecord_id} {digest_mismatch_count}")
                    mydb_utils.uga_out(fout, [satrecord_id, erws, tdate_ym, mss, tdate_ym, total, tdate_ym, vlast, vfirst, vmi, dob, vemail, vaddr1, vcity, vstate, vpostal, digparts, db_digest_nld, digest_nld])

                    #dfields = self.digparts_split_fix(digparts)
                    #print(len(dfields))
                    
                    d_ldate, d_erws, d_tdate_ym, d_mss, d_tdate_ym, d_total, d_tdate_ym, d_vlast, d_vfirst, d_vmi, d_dob, d_vemail, d_vaddr1, d_vcity, d_vstate, d_vpostal = self.digparts_split_fix(digparts)
                    digparts_fix = ",".join([d_ldate, d_erws, d_tdate_ym, d_mss, d_tdate_ym, d_total, d_tdate_ym, d_vlast, d_vfirst, d_vmi, d_dob, d_vemail, d_vaddr1, d_vcity, d_vstate, d_vpostal])
                    
                    digest_nld_fix  = sat_utils.calc_digest_nld(d_erws, d_tdate_ym, d_mss, d_tdate_ym, d_total, d_tdate_ym, d_vlast, d_vfirst, d_vmi, d_dob, d_vemail, d_vaddr1, d_vcity, d_vstate, d_vpostal)
                    print(digparts)
                    print(digparts_fix)
                    print(digest_nld)
                    print(digest_nld_fix)
                    print("")
                    update_arr.append(f"{satrecord_id}^{digparts_fix}^{digest_nld_fix}^EOA")
                    
                rowcount += 1
        print(f"digest mismatch count {digest_mismatch_count} row count {rowcount}")
        self.do_update(conn, update_arr)
        
def main():
    conn = None
    try:
        ts = mydb_utils.get_db_ts()
        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        scores_db_file = os.path.join(db_dir, scores_db_name)
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")

        conn = mydb_utils.sqlite3_connect(scores_db_file)
        b = Bork()
        #b.debug = True
        b.go(conn, ts)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()


