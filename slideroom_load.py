#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved.

import sys
import traceback
import sqlite3
import csv
import os.path
from pw_utils import mydb_utils
import re

note = """
7/10/25 incomplete
"""

class Bork:
    debug = False
    
    def __init__(self):
        pass

    def remove_nl(self, value):
        value = re.sub("\u000a"," ",value)
        value = re.sub("\u000d"," ",value)
        value = re.sub("\u00A0"," ",value)
        value = re.sub("\""," ",value)
        value = re.sub("\'"," ",value)
        
        return value
        
    def get_abbrev_prog(self,field):
        ufield = field.upper()
        ufield = re.sub("TRANSFER \- ","",ufield)
        ufield = re.sub("TRANSFER ","",ufield)
        ufield = re.sub("TRANSFER: ","",ufield)
        ufield = re.sub("COMMON APPLICATION ","",ufield)
        ufield = re.sub("COMMON APP ","",ufield)
        ufield = re.sub("THE COALITION APPLICATION ","",ufield)
        ufield = re.sub("COALITION APP ","",ufield)
        ufield = re.sub("COALITION/QUESTBRIDGE APPLICATION ","",ufield)
        
        return ufield
        
    def go(self, conn):
        if len(sys.argv[1:]) == 0:
            print("use _ filename(s).csv")
            sys.exit(-1)
        for fname in sys.argv[1:]:
            with open(fname, "r", encoding='UTF-8', newline='') as csvfile:
                reader = csv.reader(csvfile, dialect='excel')
                line_number = 0
                for row in reader:
                    # skip header
                    if line_number == 0:
                        line_number += 1
                        continue
                    vrow = list(map(self.remove_nl, row))
                    num_cols = len(vrow)
                    #print(f"num cols {num_cols} line {line_number}")
                    last, first, ceeb, email, submission_id, program, date_submitted, taglist, overall_rating, reviewer_first, reviewer_last , reviewer_email, reviewer_note, reviewer_combined_rating, date_evaluated, music_other, voice_part, common_app_id = vrow
                    #print(date_evaluated)
                    #mydb_utils.uga_out(sys.stdout, vrow)

                    abbrev_prog = self.get_abbrev_prog(program)
                    #print(f"{program:<100} {abbrev_prog}")
                    print(abbrev_prog)
                    
                    line_number += 1
                    
            pass # def go
    pass # class Bork
        
def main():
    conn = None
    try:
        slideroom_db_name = "slideroom.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        slideroom_db_file = os.path.join(db_dir, slideroom_db_name)
        if not os.path.exists(slideroom_db_file):
            raise RuntimeError(f"slideroom db file {slideroom_db_file} not found")

        conn = mydb_utils.sqlite3_connect(slideroom_db_file)
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

