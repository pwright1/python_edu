#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
from pw_utils import mydb_utils

note = """
remove header row from act.csv file before loading into PeopleSoft
the original file goes to Slate. The nohdr file goes to PS
python %PY%\act_hdr_remove.py ACT*.csv
"""

class Bork:
    debug = False
    def __init__(self):
        pass

    def go(self):
        if len(sys.argv[1:]) == 0:
            print("use python act_hdr_remove.py file(s).csv")
            print("use for files loaded to PS only. Send original files to Slate.")
            sys.exit()

        args = mydb_utils.get_glob_args()
        for fname in args:
            if len(fname) < 5:
                print (f"file {fname} filename length too short, ignoring")
                continue
            fname_base = fname[0:-4]
            fname_new = f"{fname_base}_nohdr.csv"
            #print(f"fname {fname} {fname_base} {fname_new}")
            input_done = False
            input_line_count = 0
            with open(fname, "r") as fin:
                with open(fname_new, "w") as fout:
                    while not input_done:
                        line = fin.readline()
                        # eof
                        if len(line) == 0:
                            input_done = True
                            continue
                        #blank line
                        if len(line) == 1:
                            input_line_count += 1
                            continue
                        # skip header line
                        if input_line_count == 0:
                            input_line_count += 1
                            continue
                        fout.write(line)
                        input_line_count += 1
                print(fname_new)


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

