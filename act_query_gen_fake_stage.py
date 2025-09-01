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
from pw_utils import act2025csv
from pw_utils import act2020csv
from pw_utils import act2016txt
from pw_utils import act2015txt


note = """
queries stored data to generate a stage file to simulate loadings scores
and then querying PS for the stage data. used for testing to create documentation
without PS access. 
"""

class Bork:
    debug = False
    
    def __init__(self):
        pass

    def act_insert(self, conn, q, tup):
        cur = conn.cursor()
        cur.execute(q, tup)
        lastrowid = cur.lastrowid
        return lastrowid

    def act_stage_fake_seq_q(self):
        q = """
        insert into act_stage_fake_seq (test_value) values (?)
        """
        return q

    def do_delete(self, conn):
        cur = conn.cursor()
        cur.execute("delete from act_stage_fake")
        conn.commit()
    
    def act_stage_fake_q(self):
        q = """
        insert into act_stage_fake(siss_id, test_id, test_component, test_date, data_src, score, date_loaded, last, first, middle, email, address1, city, state, postal, country, birthdate, homephone, cphone, ophone, status, created, last_update, actrecord_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        return q
    
    def go(self, conn, ts):

        digest_seen_hash = {}
        stu.act_stage_fake_table_drop(conn)
        stu.act_stage_fake_seq_table_drop(conn)

        stu.act_stage_fake_seq_table_create(conn)
        stu.act_stage_fake_table_create(conn)
        
        # select the matched records from the database. and add in the excluded ones
        # trying to simulate rematching the whole group again for doc purposes
        # you need to set the exclude env var before doing the match on this stage data output
        q = """
        select r.actrecord_id, r.etestdate, r.engl, r.math, r.read, r.scire, r.comp,
        r1.sup_eng_sc, r1.sup_eng_dt,
        r1.sup_mth_sc, r1.sup_mth_dt,
        r1.sup_rdg_sc, r1.sup_rdg_dt,
        r1.sup_sci_sc, r1.sup_sci_dt,r1.sup_composite,r1.country_iso,dob,
        r.last, r.first, r.mi, r.edob, r.phone, r.email, r.street, r.city, r.state, r.zip5,
        r.digparts, r.digest_nld
        from actrecord r, actrecord_addl_1 r1, act_matched_keys k
        where r1.actrecord_id = r.actrecord_id
        and r.actrecord_id = k.actrecord_id
        and (
        (k.match_date > '2024-10-28' and k.match_date < '2024-11-06')
        or
        (k.match_date > '2024-12-19' and k.match_date < '2025-01-17')
        )

        union 
        
        select re.actrecord_id, re.etestdate, re.engl, re.math, re.read, re.scire, re.comp,
        re1.sup_eng_sc, re1.sup_eng_dt,
        re1.sup_mth_sc, re1.sup_mth_dt,
        re1.sup_rdg_sc, re1.sup_rdg_dt,
        re1.sup_sci_sc, re1.sup_sci_dt,re1.sup_composite,re1.country_iso,dob,
        re.last, re.first, re.mi, re.edob, re.phone, re.email, re.street, re.city, re.state, re.zip5,
        re.digparts, re.digest_nld
        from actrecord re, actrecord_addl_1 re1, act_exclude x
        where re1.actrecord_id = re.actrecord_id
        and re.actrecord_id = x.actrecord_id
        and (
        (x.excldate > '2024-10-28' and x.excldate < '2024-11-06')
        or
        (x.excldate > '2024-12-19' and x.excldate < '2025-01-17')
        )

        """
        cur = conn.cursor()
        i = 0
        for row in cur.execute(q):
            actrecord_id, etestdate, engl, math, read, scire, comp, sup_eng_sc, sup_eng_dt, sup_mth_sc, sup_mth_dt, sup_rdg_sc, sup_rdg_dt, sup_sci_sc, sup_sci_dt, sup_composite, country, dob, last, first, mi, edob, phone, email, street, city, state, zip5, digparts, db_digest_nld = row

            if digest_seen_hash.get(db_digest_nld, None) == None:
                digest_seen_hash[db_digest_nld] = True
            else:
                i += 1
                print(f"dup hash detected {actrecord_id}, {last}, {first}")
                continue
            
            vdob_stage = ""
            if len(dob) == 10:
                vdob_stage = f"{dob[5:7]}/{dob[8:10]}/{dob[0:4]}"
            
            vlast = last.upper()[0:30].strip()
            vfirst = first.upper()[0:30].strip()
            vmi = mi.upper()[0:1].strip()
            vstreet = street.upper()[0:40].strip()
            vemail = email.upper()[0:50].strip()
            vcity = city.upper()[0:30].strip()

            vstate = ""
            vpostal = ""
            if country.upper() == "USA":
                vpostal = zip5[0:5].strip()
                vstate = state[0:2].strip()
                
            if vstate == "FN" or vstate == "CN":
                vstate = ""
        
            vstate = state
            vpostal = zip5
        
            # data stored in db has etestdate as YYYYMM
            etestdate_stage = f"{etestdate[4:6]}/01/{etestdate[0:4]}"
            
            vsup_eng_dt_stage = f"{sup_eng_dt[0:2]}/01/{sup_eng_dt[2:6]}"
            vsup_eng_dt = act_utils.mmyyyy_swap(sup_eng_dt)

            vsup_mth_dt_stage = f"{sup_mth_dt[0:2]}/01/{sup_mth_dt[2:6]}"
            vsup_mth_dt = act_utils.mmyyyy_swap(sup_mth_dt)

            vsup_rdg_dt_stage = f"{sup_rdg_dt[0:2]}/01/{sup_rdg_dt[2:6]}"
            vsup_rdg_dt = act_utils.mmyyyy_swap(sup_rdg_dt)

            vsup_sci_dt_stage = f"{sup_sci_dt[0:2]}/01/{sup_sci_dt[2:6]}"
            vsup_sci_dt = act_utils.mmyyyy_swap(sup_sci_dt)

            veng, veng_dt =               act_utils.score_date_check(engl, etestdate)
            vsup_eng_sc, vsup_eng_dt =    act_utils.score_date_check(sup_eng_sc, vsup_eng_dt)
            vmth, vmth_dt =               act_utils.score_date_check(math, etestdate)
            vsup_mth_sc, vsup_mth_dt =    act_utils.score_date_check(sup_mth_sc, vsup_mth_dt)
            vrdg, vrdg_dt =               act_utils.score_date_check(read, etestdate)
            vsup_rdg_sc, vsup_rdg_dt =    act_utils.score_date_check(sup_rdg_sc, vsup_rdg_dt)
            vsci, vsci_dt =               act_utils.score_date_check(scire, etestdate)
            vsup_sci_sc, vsup_sci_dt =    act_utils.score_date_check(sup_sci_sc, vsup_sci_dt)
            vcomposite, vcomposite_dt =   act_utils.score_date_check(comp, etestdate)


            digest_nld = act_utils.calc_digest_nld(veng, etestdate, sup_eng_sc, vsup_eng_dt, vmth, etestdate, vsup_mth_sc, vsup_mth_dt, vrdg, etestdate, vsup_rdg_sc, vsup_rdg_dt, vsci, etestdate, vsup_sci_sc, vsup_sci_dt, vcomposite, etestdate, sup_composite, vlast, vfirst, vmi, dob, vemail, vstreet, vcity, vstate, vpostal)

            stage_date_loaded = mydb_utils.iso_to_date(ts[0:10])

            if db_digest_nld != digest_nld:
                print(f"digest mismatch for {actrecord_id}")
                #mydb_utils.uga_out(sys.stdout, [actrecord_id, veng, etestdate, sup_eng_sc, vsup_eng_dt, vmth, etestdate, sup_mth_sc, vsup_mth_dt, vrdg, etestdate, sup_rdg_sc, vsup_rdg_dt, vsci, etestdate, sup_sci_sc, vsup_sci_dt, vcomposite, etestdate, sup_composite, vlast, vfirst, vmi, dob, vemail, vstreet, vcity, vstate, vpostal,digparts, db_digest_nld, digest_nld])

                
            #mydb_utils.uga_out(sys.stdout, [i,engl, etestdate, sup_eng_sc, vsup_eng_dt, math, etestdate, sup_mth_sc, vsup_mth_dt, read, etestdate, sup_rdg_sc, vsup_rdg_dt, scire, etestdate, sup_sci_sc, vsup_sci_dt, comp, etestdate, sup_composite, vlast, vfirst, vmi, dob, vemail, vstreet, vcity, vstate, vpostal])
            seq = self.act_insert(conn, self.act_stage_fake_seq_q(), ("test_value",))

            if veng != "" and veng_dt != "":
                tup6 = (seq, "ACT","ENGL",etestdate_stage,"ACT",veng, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","", actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)

            if vmth != "" and vmth_dt != "":
                tup6 = (seq, "ACT","MATH",etestdate_stage,"ACT",vmth, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","",actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vrdg != "" and vrdg_dt != "":
                tup6 = (seq, "ACT","READ",etestdate_stage,"ACT",vrdg, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","",actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vsci != "" and vsci_dt != "":
                tup6 = (seq, "ACT","SCIRE",etestdate_stage,"ACT",vsci, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","",actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vcomposite != "" and vcomposite_dt != "":
                tup6 = (seq, "ACT","COMP",etestdate_stage,"ACT",vcomposite, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","",actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)

            if vsup_eng_sc != "" and vsup_eng_dt != "":
                tup6 = (seq, "ACT","ENGLS",vsup_eng_dt_stage,"ACT",vsup_eng_sc, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","",actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vsup_mth_sc != "" and vsup_mth_dt != "":
                tup6 = (seq, "ACT","MATHS",vsup_mth_dt_stage,"ACT",vsup_mth_sc, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","",actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vsup_rdg_sc != "" and vsup_rdg_dt != "":
                tup6 = (seq, "ACT","READS",vsup_rdg_dt_stage,"ACT",vsup_rdg_sc, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","",actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vsup_sci_sc != "" and vsup_sci_dt != "":
                tup6 = (seq, "ACT","SCISS",vsup_sci_dt_stage,"ACT",vsup_sci_sc, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","",actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)

            if sup_composite != "":
                tup6 = (seq, "ACT","COMPS",etestdate_stage,"ACT",sup_composite, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","",actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
                
            if i % 100 == 0:
                conn.commit()
            i += 1
        conn.commit()
        
            
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
        b.do_delete(conn)
        b.go(conn, ts)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()


