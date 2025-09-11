#!/usr/bin/env python3

# Copyright Sep 2025, Philip Wright. All rights reserved. 

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
        pass

    def insert_exclude(self, conn, toefl_record_id, emplid, excl_date, scc_temp_id):
        q = """
        insert into toefl_exclude(toefl_record_id, emplid, excl_date, scc_temp_id) values (?, ?, ?, ?)
        on conflict do nothing
        """
        cur = conn.cursor()
        res = cur.execute(q, (toefl_record_id, emplid, excl_date, scc_temp_id))
        rows = cur.rowcount
        if rows != 1:
            print(f"insert failed rows: {rows} {toefl_record_id}")

    def lu_toefl_record(self, conn, filename, fileline):
        lu_q = """
        select r.toefl_record_id, r.ran, r.last, r.first
        from toefl_record r, toefl_file f
        where r.toefl_file_id = f.toefl_file_id
        and f.filename = ?
        and r.fileline = ?
        """

        cur = conn.cursor()
        toefl_record_id, ran, last, first = ["","","",""]
        row_count = 0
        for row in cur.execute(lu_q, (filename, fileline)):
            toefl_record_id, ran, last, first = row
            row_count += 1
        cur.close()
        return [row_count, toefl_record_id, ran, last, first]
    
    def go(self, conn):
        if len(sys.argv[1:]) == 0:
            print("use _ filename.csv")
            sys.exit(-1)

        fname = sys.argv[1]
        line_count = 0
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
                    continue
                # skip header
                if line_count == 0:
                    line_count += 1
                    continue
                line = line.strip()
                fields = mydb_utils.split_sq_csv_line(line)
                expected = 11
                if len(fields) != expected:
                    raise RuntimeError(f"unexpected column count {len(fields)}, should be {expected}. Exiting")
                toefl_file_id, filename, lines, toefl_record_id, fileline, ran, last, first, emplid, excl_date, scc_temp_id = fields

                row_count, db_toefl_record_id, db_ran, db_last, db_first = self.lu_toefl_record(conn, filename, fileline)

                if ran != db_ran:
                    print(f"ran mismatch {ran} {db_ran} {filename} {fileline}")

                if last != db_last or first != db_first:
                    print(f"name mismatch {last} {db_last} {first} {db_first} {filename} {fileline}")
                    
                if row_count == 0:
                    print("no rows")
                elif row_count == 1:
                    pass
                    #print(f"one row {line_count}")
                else:
                    print (f"{row_count} rows")

                #mydb_utils.uga_out(sys.stdout, [db_toefl_record_id])
                self.insert_exclude(conn, db_toefl_record_id, emplid, excl_date, scc_temp_id)
                if line_count % 100 == 0:
                    conn.commit()
                line_count += 1
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

