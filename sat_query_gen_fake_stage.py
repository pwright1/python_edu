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


note = """
queries stored data to generate a stage file to simulate loadings scores
and then querying PS for the stage data. used for testing to create documentation
without PS access. 
"""

class Bork:
    debug = False
    
    def __init__(self):
        pass

    def sat_insert(self, conn, q, tup):
        cur = conn.cursor()
        cur.execute(q, tup)
        lastrowid = cur.lastrowid
        return lastrowid

    def sat_stage_fake_seq_q(self):
        q = """
        insert into sat_stage_fake_seq (test_value) values (?)
        """
        return q

    def do_delete(self, conn):
        cur = conn.cursor()
        cur.execute("delete from sat_stage_fake")
        conn.commit()

    def sat_stage_fake_q(self):
        q = """
        insert into sat_stage_fake(siss_id, test_id, test_component, test_date, data_src, score, date_loaded, last, first, middle, email, address1, city, state, postal, country, birthdate, homephone, status, created, last_update, satrecord_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        return q
    
    def go(self, conn, ts):

        digest_seen_hash = {}
        stu.sat_stage_fake_table_drop(conn)
        stu.sat_stage_fake_seq_table_drop(conn)

        stu.sat_stage_fake_seq_table_create(conn)
        stu.sat_stage_fake_table_create(conn)
        
        # select the matched records from the database. and add in the excluded ones
        # trying to simulate rematching the whole group again for doc purposes
        # you need to set the exclude env var before doing the match on this stage data output
        q = """
        select r.satrecord_id, r.tdate_1, r.erws_1, r.mss_1, r.total_1, 
        r.last, r.first, r.mi, r.dob, r.phone, r.email, r.addr1, r.city, r.state, r.postal, r.country3,
        r.digparts, r.digest_nld
        from satrecord r, sat_matched_keys k
        where r.satrecord_id = k.satrecord_id
        and (
        (k.match_date > '2024-10-28' and k.match_date < '2024-11-06')
        or
        (k.match_date > '2024-12-19' and k.match_date < '2025-01-17')
        )

        union 

        select re.satrecord_id, re.tdate_1, re.erws_1, re.mss_1, re.total_1, 
        re.last, re.first, re.mi, re.dob, re.phone, re.email, re.addr1, re.city, re.state, re.postal, re.country3,
        re.digparts, re.digest_nld
        from satrecord re, sat_exclude x
        where re.satrecord_id = x.satrecord_id
        and (
        (x.excldate > '2024-10-28' and x.excldate < '2024-11-06')
        or
        (x.excldate > '2024-12-19' and x.excldate < '2025-01-17')
        )

        """
        cur = conn.cursor()
        i = 0
        for row in cur.execute(q):
            satrecord_id, testdate, erws, mss, total, last, first, mi, dob, phone, email, addr1, city, state, postal, country, digparts, db_digest_nld = row


            if digest_seen_hash.get(db_digest_nld, None) == None:
                digest_seen_hash[db_digest_nld] = True
            else:
                i += 1
                print(f"dup hash detected {satrecord_id}, {last}, {first}")
                continue


            vdob_stage = ""
            if len(dob) == 10:
                vdob_stage = f"{dob[5:7]}/{dob[8:10]}/{dob[0:4]}"
            
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
            testdate_stage = f"{testdate[5:7]}/01/{testdate[0:4]}"
            tdate_ym = f"{testdate[0:4]}{testdate[5:7]}"
            
            digest_nld  = sat_utils.calc_digest_nld(erws, tdate_ym, mss, tdate_ym, total, tdate_ym, vlast, vfirst, vmi, dob, vemail, vaddr1, vcity, vstate, vpostal)

            stage_date_loaded = mydb_utils.iso_to_date(ts[0:10])

            if db_digest_nld != digest_nld:
                print(f"digest mismatch for {satrecord_id}")
                mydb_utils.uga_out(sys.stdout, [erws, tdate_ym, mss, tdate_ym, total, tdate_ym, vlast, vfirst, vmi, dob, vemail, vaddr1, vcity, vstate, vpostal, digparts, db_digest_nld, digest_nld])
                
            seq = self.sat_insert(conn, self.sat_stage_fake_seq_q(), ("test_value",))

            if testdate != "" and total != "" and erws != "" and mss != "":
                self.sat_insert(conn, self.sat_stage_fake_q(),(seq, "SAT1", "TOTAL",testdate_stage, "ETS", total, stage_date_loaded, last, first, mi, email, addr1, city, vstate, vpostal, country, vdob_stage, phone,"LD", "","", satrecord_id))
                self.sat_insert(conn, self.sat_stage_fake_q(), (seq, "SAT1", "ERWS",testdate_stage, "ETS", erws, stage_date_loaded, last, first, mi, email, addr1, city, vstate, vpostal, country, vdob_stage, phone,"LD", "","",satrecord_id))
                self.sat_insert(conn, self.sat_stage_fake_q(), (seq, "SAT1", "MSS",testdate_stage, "ETS", mss, stage_date_loaded, last, first, mi, email, addr1, city, vstate, vpostal, country, vdob_stage, phone,"LD", "","",satrecord_id))
                
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


