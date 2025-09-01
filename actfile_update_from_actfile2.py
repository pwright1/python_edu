#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from pw_utils import mydb_utils
from pw_utils import score_tables_utils as stu

note = """
updates actfile(oldactfile_id, oldfilename, oloaddate, oldlines)
from selecting from actfile2(actfile_id, filename, loaddate, lines)
needed to get the old load date updated as its used in the digest calc
you have to populate actfile2 using a script here and a csv datafile
"""
class Bork:
    debug = False
    
    def __init__(self):
        pass


    def update_actfile_rec(self, conn, tup):
        q = """
        update actfile set oldactfile_id = ?, oldfilename = ?, oldloaddate = ?, oldlines = ?
        where actfile_id = ?
        """
        cur = conn.cursor()
        res = cur.execute(q, tup)

    def lu_actfile2_rec(self, conn, filename):
        q = """
        select actfile_id, loaddate, lines from actfile2
        where lower(filename) = ?
        limit 1
        """
        cur = conn.cursor()
        for row in cur.execute(q,(filename.lower(),)):
            oldactfile_id, oldloaddate, oldlines = row
            return (oldactfile_id, oldloaddate, oldlines)
        return ("","","")

    def add_leading_zero_alt_filename(altfilename):
        base = altfilename[0:-4]
        
        
    def go(self, conn):
        lookup_results_arr = []
        q = "select actfile_id, filename, altfilename from actfile"
        cur = conn.cursor()
        for row in cur.execute(q):
            actfile_id, filename, altfilename = row
            altfilebase = altfilename[0:-4]
            altfile_suffix = altfilename[-4:]
            act_, adate, aseq = altfilebase.split("_")
            aseq2 = ""
            if aseq[0] != "c":
                aseq2 = f"{int(aseq):06d}"
            altfilename2 = f"act_{adate}_{aseq2}{altfile_suffix}"
            #mydb_utils.uga_out(sys.stdout, [str(actfile_id), filename, altfilebase, altfilename2])
            fail_count = 0
            oldactfile_id, oldloaddate, oldlines = self.lu_actfile2_rec(conn, filename.lower())
            if oldactfile_id != "":
                lookup_results_arr.append((oldactfile_id, filename, oldloaddate, oldlines, actfile_id))
            oldactfile_id, oldloaddate, oldlines = self.lu_actfile2_rec(conn, altfilename.lower())
            if oldactfile_id != "":
                lookup_results_arr.append((oldactfile_id, filename, oldloaddate, oldlines, actfile_id))
            oldactfile_id, oldloaddate, oldlines = self.lu_actfile2_rec(conn, altfilename2.lower())
            if oldactfile_id != "":
                lookup_results_arr.append((oldactfile_id, filename, oldloaddate, oldlines, actfile_id))
        for i, tup, in enumerate(lookup_results_arr):
            print(i, tup)
            self.update_actfile_rec(conn, tup)
            if i % 100:
                conn.commit()
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

