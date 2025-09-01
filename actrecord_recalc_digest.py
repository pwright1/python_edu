#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from pw_utils import mydb_utils
from pw_utils import score_tables_utils as stu
from pw_utils import act_utils

class Bork:
    debug = False
    
    def __init__(self):
        pass


    def update_actrecord_rec(self, conn, tup):
        q = """
        update actrecord  set digest = ?, digparts = ?, digest_nld = ? where actrecord_id = ?
        """
        cur = conn.cursor()
        res = cur.execute(q, (tup))
        
    def go(self, conn):
        lookup_results_arr = []
        q = """
        select r1.actrecord_id, f.loaddate, r1.engl, r1.etestdate, r2.sup_eng_sc, r2.sup_eng_dt, r1.math, r1.etestdate, r2.sup_mth_sc, r2.sup_mth_dt, r1.read, r1.etestdate, r2.sup_rdg_sc, r2.sup_rdg_dt, r1.scire, r1.etestdate, r2.sup_sci_sc, r2.sup_sci_dt, r1.comp, r1.etestdate, r2.sup_composite,r1.last, r1.first, r1.mi, r1.edob, r1.email, r1.street, r1.city, r1.state, r1.zip5 
        from actfile f, actrecord r1
        left outer join actrecord_addl_1 r2
        on r2.actrecord_id = r1.actrecord_id
        where r1.actfile_id = f.actfile_id
        """
        cur = conn.cursor()
        for row in cur.execute(q):
            actrecord_id, loaddate, engl, engl_dt, engls, engls_dt, math, math_dt, maths, maths_dt, read, read_dt, reads, reads_dt, scire, scire_dt, sciss, sciss_dt, comp, comp_dt, comps, last, first, mi, edob, email, street, city, state, zip5 = row

            last = last.strip()
            first = first.strip()
            mi = mi.strip()
            edob = edob.strip()
            email = email.strip()
            street = street.strip()
            city = city.strip()
            state = state.strip()
            zip5 = zip5.strip()

            if state == "FN":
                state = ""
            
            vengls_dt = act_utils.mmyyyy_swap(engls_dt)
            vmaths_dt = act_utils.mmyyyy_swap(maths_dt)
            vreads_dt = act_utils.mmyyyy_swap(reads_dt)
            vsciss_dt = act_utils.mmyyyy_swap(sciss_dt)

            vengl, vengl_dt =             act_utils.score_date_check(engl, engl_dt)
            vengls, vengls_dt =           act_utils.score_date_check(engls, vengls_dt)
            vmath, vmath_dt =             act_utils.score_date_check(math, math_dt)
            vmaths, vmaths_dt =           act_utils.score_date_check(maths, vmaths_dt)
            vread, vread_dt =             act_utils.score_date_check(read, read_dt)
            vreads, vreads_dt =           act_utils.score_date_check(reads, vreads_dt)
            vscire, vscire_dt =           act_utils.score_date_check(scire, scire_dt)
            vsciss, vsciss_dt =           act_utils.score_date_check(sciss, vsciss_dt)
            vcomp, vcomp_dt =             act_utils.score_date_check(comp, comp_dt)

            #mydb_utils.uga_out(sys.stdout, [actrecord_id, loaddate, ""])
            
            
            digparts, hexdigest = act_utils.calc_digest(loaddate, vengl, vengl_dt, vengls, vengls_dt, vmath, vmath_dt, vmaths, vmaths_dt, vread, vread_dt, vreads, vreads_dt, vscire, vscire_dt, vsciss, vsciss_dt, vcomp, vcomp_dt, comps, last, first, mi, edob, email, street, city, state, zip5)

            hexdigest_nld = act_utils.calc_digest_nld(vengl, vengl_dt, vengls, vengls_dt, vmath, vmath_dt, vmaths, vmaths_dt, vread, vread_dt, vreads, vreads_dt, vscire, vscire_dt, vsciss, vsciss_dt, vcomp, vcomp_dt, comps, last, first, mi, edob, email, street, city, state, zip5)
            
            lookup_results_arr.append((hexdigest, digparts, hexdigest_nld, actrecord_id))
        cur.close

        for i, tup in enumerate(lookup_results_arr):
            self.update_actrecord_rec(conn, tup)
            if i % 1000 == 0:
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
        b = Bork()
        #b.debug = True
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()

