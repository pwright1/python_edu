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

    def update(self, conn, tc, emplid, appno):
        update_q = """
        update applicant_biod set test_consider = ?
        where emplid = ? and appno = ?
        """
        cur = conn.cursor()
        cur.execute(update_q, (tc, emplid, appno))
        pass

    def go(self, conn):
        if len(sys.argv[1:]) == 0:
            print("use _ filename.csv")
            sys.exit()
        fname = sys.argv[1]
        with open(fname, "r", encoding="UTF-8") as file:
            done = False
            line_count = 0
            expected = 7
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
                    raise RuntimeError(f"field count {fields} not expected {expected}")
                (slateid, slateappno, emplid, appno, tconsider, toptional, nplan) = fields
                self.update(conn, tconsider, emplid, appno)
                if line_count % 500 == 0:
                    print(line_count)
                    conn.commit()
                line_count += 1
                pass #while
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
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
main()

