#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
from pw_utils import mydb_utils

class Bork:
    debug = False
    def __init__(self):
        pass

    def go(self):
        num_args = len(sys.argv[1:])
        if num_args == 2:
            dialect = sys.argv[1]
            fname = sys.argv[2]
            if dialect == "standard":
                dialect = ""
            elif dialect != "excel":
                print(f"unknown dialect {dialect}")
                sys.exit(1)
            mydb_utils.csv_to_xlsx(fname, dialect)
            
        elif num_args == 3:
            dialect = sys.argv[1]
            fname = sys.argv[2]
            json = sys.argv[3]

            if dialect == "standard":
                dialect = ""
            elif dialect != "excel":
                print(f"unknown dialect {dialect}")
                sys.exit(1)
            #print("using json")
            mydb_utils.csv_to_xlsx(fname, dialect, json)

        else:
            print("use csv_to_xlsx.py dialect(standard | excel) csv_file [json_file]")
            print("python csv_to_xlsx.py standard test.csv")
            print("python csv_to_xlsx.py excel test.csv")
            print("python csv_to_xlsx.py standard test.csv test.json")
            print("python csv_to_xlsx.py excel test.csv test.json")
            sys.exit()
        
    pass # class Bork

def main():
    try:
        b = Bork()
        b.go()
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        pass
main()

