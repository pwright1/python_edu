#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
from pw_utils import mydb_utils
from pw_utils import string_utils

note = """
"""

class Bork:
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        self.py = mydb_utils.python_name()
        
    def go(self):
        if not os.path.exists("sat_ready.csv_plain.csv"):
            raise RuntimeError("sat_ready.csv_plain.csv DNE, run failsafe script first")
        if not os.path.exists("sat_excl.csv_plain.csv"):
            raise RuntimeError("sat_excl.csv_plain.csv DNE, run failsafe script first")

        cmd = f"{self.py} " + os.path.join(self.script_dir, "sat_susp_update.py sat_ready.csv_plain.csv")
        ret = os.system(cmd)

        cmd = f"{self.py} " + os.path.join(self.script_dir, "sat_exclude_load.py sat_excl.csv_plain.csv")
        ret = os.system(cmd)
         
def main():
    try:
        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        scores_db_file = os.path.join(db_dir, scores_db_name)
        
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")
        b = Bork()
        b.go()
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
        
main()

