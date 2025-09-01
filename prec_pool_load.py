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
        pass

    def insert(self, conn, tup):
        q = """
        insert into prec_pool (emplid, last, first, middle, pref, suffix, dob, nid, a1, a2, city, state, postal, country, phone, email, term, year, sex) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        on conflict do nothing
        """
        cur = conn.cursor()
        cur.execute(q, (tup))
        return
        
    def go(self, conn):
        if len(sys.argv[1:]) != 1:
            print("use prec_pool_load.py file.csv")
            sys.exit(-1)

        fname = sys.argv[1]
        expected_fields = 19
        with open(fname, "r") as file:
            line_count = 0
            done = False
            while not done:
                line = file.readline()
                if len(line) == 0:
                    done = True
                    continue
                # skip header line
                if line_count == 0:
                    line_count += 1
                    continue
                vline = line.strip()
                fields = mydb_utils.split_sq_csv_line(vline)
                nfields = len(fields)
                if nfields != expected_fields:
                    print("field count {} not expected {}".format(nfields, expected_fields))
                    sys.exit(1)
                emplid, last, first, middle, pref, suffix, dob, nid, a1, a2, city, state, postal, country, phone, email, term, year, sex = fields

                if len(nid) == 9 and nid[0:1] == "X":
                    nid = ""
                    
                tup = (emplid, last, first, middle, pref, suffix, dob, nid, a1, a2, city, state, postal, country, phone, email, term, year, sex)
                self.insert(conn, tup)
                line_count += 1
                if line_count % 500 == 0:
                    print(line_count)
                    conn.commit()
                pass # while
            conn.commit()
            pass # with
        pass # def go
    pass # class Bork
        
def main():
    conn = None
    try:
        pools_db_name = "pools.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        pools_db_file = os.path.join(db_dir, pools_db_name)
        if not os.path.exists(pools_db_file):
            raise RuntimeError(f"pools db file {scores_db_file} not found")

        conn = mydb_utils.sqlite3_connect(pools_db_file)
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

