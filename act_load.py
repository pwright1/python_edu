#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from datetime import datetime
from pw_utils import mydb_utils
from pw_utils import act_utils
from pw_utils import score_tables_utils as stu
from pw_utils import act2025csv
from pw_utils import act2020csv
from pw_utils import act2016txt
from pw_utils import act2015txt

note = """
your lowercase load files don't have header rows in them (csv)
"""

class Bork:
    debug = False
    
    def __init__(self):
        pass

    def actrecord_insert_csv(self, conn, fname, line, line_count, actfile_id, ts):
        fields = mydb_utils.split_sq_csv_line(line)
        nfields = len(fields)
        #print(f"line {line_count} fields {nfields}")

        if nfields == 375:
            inst = act2025csv.Act2025csv()
            inst.process_row(conn, fname, line, line_count, actfile_id, ts, fields)
        elif nfields == 374:
            inst = act2020csv.Act2020csv()
            inst.process_row(conn, fname, line, line_count, actfile_id, ts, fields)
        else:
            raise RuntimeError(f"file: {fname} has unexpected columns {nfields}")
        pass

    def actrecord_insert_txt(self, conn, fname, line, line_count, actfile_id, ts):
        length = len(line)
        if length != 1051 and length != 1044: # (includes newline at end)
            raise RuntimeError(f"file: {fname} has unexpected line length {length}")
        reportyear = int(line[0:2])
        etestdate = line[226:232]
        #print(f"rptyr: {reportyear} etestdt: {etestdate}")
        
        if reportyear >= 16 and reportyear <= 20:
            if etestdate < "201609":
                inst = act2015txt.Act2015txt()
                inst.process_row(conn, fname, line, line_count, actfile_id, ts)
            else:
                inst = act2016txt.Act2016txt()
                inst.process_row(conn, fname, line, line_count, actfile_id, ts)
        else:
            print(f"act txt file {fname} has unexpected report year {reportyear}, skipping row")
        
    def act_file_update_lines(self, conn, actfile_id, lines):
        q = "update actfile set lines = ? where actfile_id = ?"
        cur = conn.cursor()
        tup = (lines, actfile_id)
        cur.execute(q, tup)
        #conn.commit()
        
    def act_file_insert(self, conn, fbase, ts):
        q = "insert into actfile(filename, altfilename, loaddate) values (?,?,?)"
        cur = conn.cursor()
        altfilename, code, msg = act_utils.rename_act_file(fbase)
        if code != 0:
            raise Exception(f"act_load rename_act_file error code {code} msg {msg}")
        tup = (fbase,altfilename,ts)
        cur.execute(q, tup)
        #conn.commit()
        actfile_id = cur.lastrowid
        return actfile_id
    
    def go(self, conn):
        if len(sys.argv[1:]) == 0:
            print("use _ filename(s).csv")
            sys.exit(-1)

        args = mydb_utils.get_glob_args()
        for fname in args:
            basename = os.path.basename(fname)
            ts = mydb_utils.get_db_ts()
            if len(fname) < 4:
                print(f"skipping file {fname} with length {len(fname)}")
                continue

            file_suffix = fname[-4:]
            if file_suffix == ".csv":
                pass
            elif file_suffix == ".txt":
                pass
            else:
                print(f"skipping file {fname} with unknown suffix {file_suffix}")
                continue
            print("reading file {}".format(fname))
            actfile_id = self.act_file_insert(conn, basename, ts)
            line_count = 1
            with open(fname, "r", encoding="UTF-8") as file:
                if file_suffix == ".csv":
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
                        line = line.strip()
                        #skip header line
                        if line[0:10].upper() == "\"DATA_SRC\"":
                            continue
                        self.actrecord_insert_csv(conn, fname, line, line_count, actfile_id, ts)
                        if (line_count % 100) == 0:
                            print(line_count)
                            #conn.commit()
                        line_count += 1
                elif file_suffix == ".txt":
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
                        self.actrecord_insert_txt(conn, fname, line, line_count, actfile_id, ts)
                        if (line_count % 100) == 0:
                            print(line_count)
                            #conn.commit()
                        line_count += 1
            self.act_file_update_lines(conn, actfile_id, line_count-1)
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

