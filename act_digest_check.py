#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from datetime import datetime
from pw_utils import mydb_utils
from pw_utils import act_utils
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
        q = "update actrecord set digparts = ?, digest_nld = ? where actrecord_id = ?"
        cur = conn.cursor()
        for i, row in enumerate(arr):
            actrecord_id, digparts, digest_nld, eoa = row.split("^")
            mydb_utils.uga_out(sys.stdout, [i, actrecord_id, digest_nld])
            cur.execute(q, (digparts, digest_nld, actrecord_id))
        conn.commit()

    def go(self, conn, ts):
        update_arr = []
        digest_mismatch_count = 0
        q = """
        select r.actrecord_id, r.etestdate, r.engl, r.math, r.read, r.scire, r.comp, r1.sup_eng_sc, r1.sup_eng_dt, r1.sup_mth_sc, r1.sup_mth_dt, r1.sup_rdg_sc, r1.sup_rdg_dt, r1.sup_sci_sc, r1.sup_sci_dt, r1.sup_composite, r1.l_name, r1.f_name, r1.m_initial, r1.dob, r1.email, r1.address1, r1.city, r1.state, r1.zip5, r1.country_iso, r.digparts, r.digest_nld
        from actrecord r, actrecord_addl_1 r1
        where r.actrecord_id = r1.actrecord_id
        """
        hdr = string_utils.pct_w("actrecord_id last first mi dob email addr1 city state postal digparts db_digest_nld digest_nld")
        cur = conn.cursor()
        i = 0
        rowcount = 0
        ofile = "act_digest_out.csv"
        with open(ofile, "w") as fout:
            mydb_utils.uga_out(fout, hdr)
            for row in cur.execute(q):
                actrecord_id, etestdate, engl, math, read, scire, comp, sup_eng_sc, sup_eng_dt, sup_mth_sc, sup_mth_dt, sup_rdg_sc, sup_rdg_dt, sup_sci_sc, sup_sci_dt, sup_composite, last, first, mi, dob, email, addr1, city, state, postal, country, digparts, db_digest_nld = row

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

                vetestdate = etestdate
                    
                vsup_eng_dt = act_utils.mmyyyy_swap(sup_eng_dt)
                vsup_mth_dt = act_utils.mmyyyy_swap(sup_mth_dt)
                vsup_rdg_dt = act_utils.mmyyyy_swap(sup_rdg_dt)
                vsup_sci_dt = act_utils.mmyyyy_swap(sup_sci_dt)

                veng, veng_dt =               act_utils.score_date_check(engl, vetestdate)
                vsup_eng_sc, vsup_eng_dt =    act_utils.score_date_check(sup_eng_sc, vsup_eng_dt)
                vmth, vmth_dt =               act_utils.score_date_check(math, vetestdate)
                vsup_mth_sc, vsup_mth_dt =    act_utils.score_date_check(sup_mth_sc, vsup_mth_dt)
                vrdg, vrdg_dt =               act_utils.score_date_check(read, vetestdate)
                vsup_rdg_sc, vsup_rdg_dt =    act_utils.score_date_check(sup_rdg_sc, vsup_rdg_dt)
                vsci, vsci_dt =               act_utils.score_date_check(scire, vetestdate)
                vsup_sci_sc, vsup_sci_dt =    act_utils.score_date_check(sup_sci_sc, vsup_sci_dt)
                vcomp, vcomp_dt =   act_utils.score_date_check(comp, vetestdate)
                
                digest_nld  = act_utils.calc_digest_nld(veng, veng_dt, vsup_eng_sc, vsup_eng_dt, vmth, vmth_dt, vsup_mth_sc, vsup_mth_dt, vrdg, vrdg_dt, vsup_rdg_sc, vsup_rdg_dt, vsci, vsci_dt, vsup_sci_sc, vsup_sci_dt, vcomp, vcomp_dt, sup_composite, vlast, vfirst, vmi, dob, vemail, vaddr1, vcity, vstate, vpostal)

                if db_digest_nld != digest_nld:
                    digest_mismatch_count += 1
                    fields = digparts.split(",")
                    d_loaddate = fields[0]
                    print(f"digest mismatch for {actrecord_id} {digest_mismatch_count} {d_loaddate}")
                    print("")

                    digparts_fix_arr = []
                    digparts_fix_arr.append(d_loaddate)
                    for val in [veng, veng_dt, vsup_eng_sc, vsup_eng_dt, vmth, vmth_dt, vsup_mth_sc, vsup_mth_dt, vrdg, vrdg_dt, vsup_rdg_sc, vsup_rdg_dt, vsci, vsci_dt, vsup_sci_sc, vsup_sci_dt, vcomp, vcomp_dt, sup_composite, vlast, vfirst, vmi, dob, vemail, vaddr1, vcity, vstate, vpostal]:
                        digparts_fix_arr.append(val)
                    
                    digparts_fix = ",".join(digparts_fix_arr)
                    print(digparts)
                    print(digparts_fix)
                    print()

                    update_arr.append(f"{actrecord_id}^{digparts_fix}^{digest_nld}^EOA")
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


