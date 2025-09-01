#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from datetime import datetime
from pw_utils import mydb_utils
from pw_utils import score_tables_utils


class Bork:
    debug = False
    def __init__(self):
        self.index_fields = [
            "first^upper(first)",
            "fi^upper(substr(first,1,1))",
            "middle^upper(middle)",
            "appno^appno",
            "hcity^upper(hcity)",
            "ceeb^ceeb",
            "last^upper(last)",
            "mcity^upper(mcity)",
            "scity^upper(scity)",
            "emplid^emplid",
            "mpostal^upper(mpostal)",
            "hpostal^upper(hpostal)",
            "mpostal2^upper(substr(mpostal,1,5))",
            "hpostal2^upper(substr(hpostal,1,5))",
            "email^upper(email)",
            "birthdate^birthdate",
            # "phone^phone",
            "slateid^slateid",
            "orgid^length(orgid)",
            ]
        pass

    def create_index(self, conn):
        for field in self.index_fields:
            fname, fname2 = field.split("^")
            index_q = f"create index ap_biod_idx_{fname} on applicant_biod ({fname2})"
            #print(index_q)
            cur = conn.cursor()
            cur.execute(index_q)
    
    def drop_index(self, conn):
        for field in self.index_fields:
            fname, fname2 = field.split("^")
            index_q = f"drop index if exists ap_biod_idx_{fname}"
            #print(index_q)
            cur = conn.cursor()
            cur.execute(index_q)
    
    def insert(self, conn, tup):
        insert_q = """
        insert into applicant_biod (SLATEID, EMPLID, APPNO, NPLAN, ADMITTYPE, ADMITTERM, FIRST, MIDDLE, LAST, PREFNAME, SUFFIX, SEX, BIRTHDATE, EMAIL, FATHEREMAIL, MOTHEREMAIL, CELLPHONE, HOMEPHONE, FATHERPHONE, MOTHERPHONE, MADDRESS1, MADDRESS2, MADDRESS3, MADDRESS4, MCITY, MSTATE, MPOSTAL, MCOUNTRY, MCOUNTRYDE, HADDRESS1, HADDRESS2, HADDRESS3, HADDRESS4, HCITY, HSTATE, HPOSTAL, HCOUNTRY, HCOUNTRYDE, SNAME, SADDRESS1, SADDRESS2, SCITY, SSTATE, SPOSTAL, SCOUNTRY, ORGID, CEEB) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        on conflict do nothing
        """
        cur = conn.cursor()
        cur.execute(insert_q, tup)
        pass

    def go(self, conn):
        if len(sys.argv[1:]) == 0:
            print("use _ filename.csv")
            sys.exit()
        fname = sys.argv[1]
        with open(fname, "r", encoding="UTF-8") as file:
            line_count = 0
            expected = 47
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
                if nfields != expected:
                    raise Exception(f"field count {fields} not expected {expected}")
                (slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, cellphone, homephone, fatherphone, motherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, mpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, hpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, spostal, scountry, orgid, ceeb) = fields
                self.insert(conn, fields)
                if line_count % 300 == 0:
                    print(line_count)
                    conn.commit()
                line_count += 1
                pass # while
            conn.commit()
            pass # with
        pass # go
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
        score_tables_utils.applicant_biod_table_drop(conn)
        score_tables_utils.applicant_biod_table_create(conn)
        b.go(conn)
        b.create_index(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
main()

