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

    def delete(self, conn):
        q = "delete from org"
        cur = conn.cursor()
        cur.execute(q)

    def insert(self, conn, tup):
        q = """
        insert into org (orgid, orgtype, atp, a1, a2, a3, a4, city, state, postal, country, name, estatus, edate, ls_sch_type, org_location) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        on conflict do nothing
        """
        cur = conn.cursor()
        cur.execute(q, tup)
        return
        
    def go(self, conn):
        if len(sys.argv[1:]) != 1:
            print("use org_load.py file.csv")
            sys.exit(-1)

        self.delete(conn)
        fname = sys.argv[1]
        expected_fields = 16
        with open(fname, "r", encoding='ISO-8859-1') as file:
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
                orgid, orgtype, atp, a1, a2, a3, a4, city, state, postal, country, name, estatus, edate, ls_sch_type, org_location = fields
                tup = (orgid, orgtype, atp, a1, a2, a3, a4, city, state, postal, country, name, estatus, edate, ls_sch_type, org_location)
                self.insert(conn, tup)
                line_count += 1
                if line_count % 1000 == 0:
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
        apps_db_name = "apps.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        apps_db_file = os.path.join(db_dir, apps_db_name)
        if not os.path.exists(apps_db_file):
            raise RuntimeError(f"apps db file {scores_db_file} not found")

        conn = mydb_utils.sqlite3_connect(apps_db_file)
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

