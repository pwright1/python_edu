#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
from pw_utils import mydb_utils

note = """
"""

class Bork:
    debug = False
    
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        self.py = mydb_utils.python_name()
        pass

    def clear_query_pass(self, conn):
        q = "update satstage set query_pass = 0"
        cur = conn.cursor()
        res = cur.execute(q)
        conn.commit()

    def go(self, conn):
        if len(sys.argv[1:]) != 1:
            print("use sat_matches.py 1 or both")
            sys.exit(-1)
            
        self.clear_query_pass(conn)
        # check that test consider is populated here
        which_match = sys.argv[1]
        
        if which_match in ["1","both"]:
            ts_start = mydb_utils.get_db_ts()
            cmd1 = f"{self.py} " + os.path.join(self.script_dir,"sat_match.py 1")
            ret = os.system(cmd1)
            ts_end = mydb_utils.get_db_ts()
            print(f" 1 Started {ts_start} Ended: {ts_end}")

        if which_match in ["both"]:
            ts_start = mydb_utils.get_db_ts()
            cmd2 = f"{self.py} " + os.path.join(self.script_dir, "sat_match.py 2")
            ret = os.system(cmd2)
            ts_end = mydb_utils.get_db_ts()
            print(f" 2 Started {ts_start} Ended: {ts_end}")

        self.load_db(which_match)
        self.query_db(which_match)
        
        pass # def go

    def load_db(self, which_match):
        # delete data from previous match
        cmd = f"{self.py} " + os.path.join(self.script_dir, "sat_match_truncate.py")
        ret = os.system(cmd)

        #if which_match in ["1","both"]:
        if True:
            if os.path.exists("sat_unmatched1.txt"):
                # you need to put the python exec first in line...
                cmd = f"{self.py} " + os.path.join(self.script_dir, "sat_match_insert.py u1 sat_unmatched1.txt")
                print(cmd)
                ret = os.system(cmd)
        
            if os.path.exists("sat_ready1.txt"):
                cmd = f"{self.py} " + os.path.join(self.script_dir, "sat_match_insert.py r1 sat_ready1.txt")
                print(cmd)
                ret = os.system(cmd)

        #if which_match in ["2","both"]:
        if True:
            if os.path.exists("sat_unmatched2.txt"):
                cmd = f"{self.py} " + os.path.join(self.script_dir, "sat_match_insert.py u2 sat_unmatched2.txt")
                print(cmd)
                ret = os.system(cmd)
        
            if os.path.exists("sat_ready2.txt"):
                cmd = f"{self.py} " + os.path.join(self.script_dir, "sat_match_insert.py r2 sat_ready2.txt")
                print(cmd)
                ret = os.system(cmd)
    
    def query_db(self, which_match):
        #cmd = f"{self.py} " + os.path.join(self.script_dir, f"sat_match_results.py {which_match}")
        cmd = f"{self.py} " + os.path.join(self.script_dir, f"sat_match_results.py both")
        ret = os.system(cmd)

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

