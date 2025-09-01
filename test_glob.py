#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
from pw_utils import mydb_utils
from glob import glob
import re

class Bork:
    debug = False
    def __init__(self):
        pass

    def go(self):
        result_args = []
        if len(sys.argv[1:]) == 0:
            print("use _ file(s) ")
            sys.exit()

        args = mydb_utils.get_glob_args()
        for i, arg in enumerate(args):
            print(f"{i:3} {arg}")
            
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

