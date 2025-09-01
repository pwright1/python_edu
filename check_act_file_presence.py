#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

#import os
import sys
import os
import sqlite3
import traceback
from pw_utils import mydb_utils

class Bork:
    debug = False
    def __init__(self):
        pass

    def go(self, conn):
        files_q = """
        select filename, altfilename from actfile
        order by altfilename
        """
        cur = conn.cursor()
        for row in cur.execute(files_q):
            filename, altfilename = row
            filename_gpg = f"{filename}.gpg"
            if not os.path.exists(filename_gpg):
                print(f"DNE {filename} {altfilename}")
        cur.close()
            
def main():
    try:

        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        scores_db_file = os.path.join(db_dir, scores_db_name)
        
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")
        
        conn = mydb_utils.sqlite3_connect(scores_db_file)

        b = Bork()
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        pass
main()

