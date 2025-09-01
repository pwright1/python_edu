#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import os.path
from pw_utils import mydb_utils
import csv

note = """
"""

class Bork:
    debug = False
    
    def __init__(self):
        pass

    def go(self):
        if len(sys.argv[1:]) == 0:
            print("use excel_csv_to_plain_csv filename.csv")
            sys.exit()
        fname = sys.argv[1]
        outname = f"{fname}_plain.csv"
        fout = open(outname, "w", encoding="UTF-8")
        with open(fname, "r", encoding="UTF-8") as csvfile:
            reader = csv.reader(csvfile, dialect = 'excel', delimiter=',', quotechar='"')
            i = 0
            for row in reader:
                num_cols = len(row)
                #print(f" {i} {num_cols}")
                mydb_utils.uga_out(fout, row)
                i += 1
                
        fout.close()
    
    pass # class Bork
        
def main():
    conn = None
    try:
        b = Bork()
        b.go()
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()

