#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from datetime import datetime
from pw_utils import mydb_utils


class Bork:
    debug = False
    def __init__(self):
        pass

    def lu_record(self, conn, digest_nld):
        q = """
        select r.actrecord_id, f.actfile_id
        from actrecord r, actfile f
        where r.actfile_id = f.actfile_id
        and r.digest_nld = ?
        """
        tup = (digest_nld,)
        cur = conn.cursor()
        for row in cur.execute(q, tup):
            actrecord_id, actfile_id = row
            mydb_utils.uga_out(sys.stdout, [actrecord_id, actfile_id, digest_nld])
    
    def go(self, conn):
        q = """
        select 
        --count(*) 
        s.scc_temp_id, s.digparts, s.date_loaded, f.actfile_id, f.loaddate, upper(s.last_name), upper(s.first_name), s.birthdate, s.digest_nld
        from actstage s, actrecord r, actfile f
        where s.digest_nld = r.digest_nld
        and r.actfile_id = f.actfile_id
        and not exists (select 'X' from actrecord r2
        where r2.digest = s.digest)
        order by s.digest_nld, s.date_loaded
        --limit 100
        """
        hdr = ["scc_temp_id", "digparts", "s_date_loaded", "f_actfile_id","f_loaddate", "last", "first", "dob", "digest_nld"]
        
        outname = "stage_dig_comp.csv"
        with open(outname, "w", encoding="UTF-8") as fout:
            mydb_utils.uga_out(fout, hdr)
            cur = conn.cursor()
            for row in cur.execute(q):
                scc_temp_id, digparts, s_date_loaded, f_actfile_id, f_loaddate, last, first, dob, digest_nld = row
                last = last.strip()
                first = first.strip()
                #self.lu_record(conn, digest_nld)
                mydb_utils.uga_out(fout, [scc_temp_id, digparts, s_date_loaded, f_actfile_id, f_loaddate, last, first, dob, digest_nld])
        
            
    pass # class Bork

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

