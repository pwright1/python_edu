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

class Bork:
    debug = False
    
    def __init__(self):
        pass
    
    def go(self):
        if len(sys.argv[1:]) < 4:
            print("use _ date_str date_str2 dst_folder filename(s).csv")
            sys.exit(-1)
        date_str = sys.argv[1]
        date_str2 = sys.argv[2]
        dst_folder = sys.argv[3]
        mydb_utils.uga_out(sys.stdout, [date_str, dst_folder])
        
        for fname in sys.argv[3:]:
            renamed_fname, code, msg = act_utils.rename_act_file(fname)
            #if renamed_fname >= date_str and renamed_fname <= date_str2:
                #move_cmd = f"mv {fname} {dst_folder}"
                #os.system(move_cmd)
            mydb_utils.uga_out(sys.stdout,[renamed_fname, code, msg, fname])
                #os.system(f"/home/hib/ruby/num_cols4.rb {fname}")
                
def main():
    conn = None
    try:
        b = Bork()
        #b.debug = True
        b.go()
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        pass
        
main()
