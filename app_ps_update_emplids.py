#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import csv
from pw_utils import mydb_utils

note = """
"""

class Bork:
    debug = False
    
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        pass

    def db_update(self, conn,slate_person_id, slate_appl_nbr, emplid, appno, ps_update_date):
        q = """
        update app_ps_main
        set emplid = ?, appno = ?, ps_update_date = ?
        where slate_person_id = ? and slate_appl_nbr = ?
        """
        cur = conn.cursor()
        res = cur.execute(q, (emplid, appno, ps_update_date, slate_person_id, slate_appl_nbr))
        rowcount = res.rowcount
        if rowcount != 1:
            print(f"update failed for slate id {slate_person_id}")
        
        
    def go(self, conn):
        if len(sys.argv[1:]) != 1:
            print("use app_ps_update_emplids.py file.csv")
            sys.exit(-1)
        
        fname = sys.argv[1]
        expected_fields = 14
        ps_update_date = mydb_utils.get_db_ts()
        
        # first pass to detect field count errors
        with open(fname, "r") as file:
            reader = csv.reader(file, dialect='excel')
            line_number = 0
            for row in reader:
                num_cols = len(row)
                if num_cols != expected_fields:
                    raise RuntimeError(f"File line {line_number+1}: number of columns {num_cols} not expected {expected_fields}")
                #print(f"{line_number} {num_cols}")
                line_number += 1
            pass # with

        with open(fname, "r") as file:
            reader = csv.reader(file, dialect='excel')
            line_number = 0
            for row in reader:
                if line_number == 0:
                    line_number += 1
                    continue
                i = 0
                transaction                             = row[i]; i += 1
                temp_id                                 = row[i]; i += 1
                status                                  = row[i]; i += 1
                load_date                               = row[i]; i += 1
                ext_sys_id                              = row[i]; i += 1 # slate_id
                ext_appno                               = row[i]; i += 1 # slate_appno
                staged_emplid                           = row[i]; i += 1
                staged_appno                            = row[i]; i += 1
                created_on                              = row[i]; i += 1
                created_by                              = row[i]; i += 1
                trans_status                            = row[i]; i += 1
                loaded_emplid                           = row[i]; i += 1
                loaded_appno                            = row[i]; i += 1
                vendor                                  = row[i]; i += 1

                #mydb_utils.uga_out(sys.stdout, [temp_id,status,ext_sys_id,ext_appno,loaded_emplid, loaded_appno])
                
                self.db_update(conn, ext_sys_id, ext_appno,loaded_emplid, loaded_appno, ps_update_date)
                               
                if line_number % 100 == 0:
                    print(line_number)
                    conn.commit()
                line_number += 1
            conn.commit()
            pass # with
        pass # def go
    pass # class
        
def main():
    conn = None
    try:
        apps_db_name = "apps.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        apps_db_file = os.path.join(db_dir, apps_db_name)
        if not os.path.exists(apps_db_file):
            raise RuntimeError(f"apps db file {apps_db_file} not found")

        conn = mydb_utils.sqlite3_connect(apps_db_file)
        b = Bork()
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()
