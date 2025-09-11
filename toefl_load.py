#!/usr/bin/env python3

# Copyright Sep 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
#import time
from datetime import datetime
from pw_utils import mydb_utils
from pw_utils import toefl_utils
from pw_utils import toefl_record


note = """
"""

class Bork:
    debug = False
    
    def __init__(self):
        pass

    def toefl_record_insert_txt(self, conn, fname, line, line_count, toefl_file_id, ts):
        t = toefl_record.ToeflRecord()
        t.set_fields_from_line(line, line_count, toefl_file_id, fname)
        insert_q = t.insert_toefl_sql()
        data_fields = tuple(t.toArr())
        cur = conn.cursor()
        cur.execute(insert_q, data_fields)
        toefl_record_id = cur.lastrowid
        #print(f"record id {toefl_record_id}")
        
    def toefl_file_update_lines(self, conn, toefl_file_id, lines):
        q = "update toefl_file set lines = ? where toefl_file_id = ?"
        cur = conn.cursor()
        tup = (lines, toefl_file_id)
        cur.execute(q, tup)
        #conn.commit()
        
    def toefl_file_insert(self, conn, fbase, ts):
        q = "insert into toefl_file(filename, loaddate) values (?,?)"
        cur = conn.cursor()
        tup = (fbase,ts)
        cur.execute(q, tup)
        toefl_file_id = cur.lastrowid
        #conn.commit()
        #print(f"file id {toefl_file_id}")
        return toefl_file_id
    
    def go(self, conn):
        if len(sys.argv[1:]) == 0:
            print("use _ filename(s).txt")
            sys.exit(-1)

        args = mydb_utils.get_glob_args()
        for fname in args:
            basename = os.path.basename(fname)
            ts = mydb_utils.get_db_ts()
            if len(fname) < 4:
                print(f"skipping file {fname} with length {len(fname)}")
                continue

            file_suffix = fname[-4:]
            if file_suffix == ".txt":
                pass
            else:
                print(f"skipping file {fname} with unknown suffix {file_suffix}")
                continue
            if basename[0:2] != "20":
                print(f"skipping file {fname} because it should start with 4 digits of a year")
                continue
                      
            print(f"reading file {fname}")
            toefl_file_id = self.toefl_file_insert(conn, basename, ts)
            line_count = 1
            with open(fname, "r", encoding="UTF-8") as file:
                done = False
                while not done:
                    line = file.readline()
                    # eof
                    if len(line) == 0:
                        done = True
                        continue
                    # blank line
                    if len(line) == 1:
                        line_count += 1
                        continue
                    #line = line.strip()  # not use on .txt files...
                    self.toefl_record_insert_txt(conn, fname, line, line_count, toefl_file_id, ts)
                    if (line_count % 100) == 0:
                        print(line_count)
                        #conn.commit()
                    line_count += 1
            self.toefl_file_update_lines(conn, toefl_file_id, line_count-1)
            # one commit per file completion
            if os.environ.get("TOE_NO_COMMIT", None) == None:
                conn.commit()
        pass # def go
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

