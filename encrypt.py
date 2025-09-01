#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
from pw_utils import encryption
from pw_utils import mydb_utils


class Bork:
    debug = False
    def __init__(self):
        pass

    def go(self):
        if len(sys.argv[1:]) == 0:
            print(f"use {sys.argv[0]} file(s)")
            sys.exit()

        args = mydb_utils.get_glob_args()
        for filename in args:
            print(filename)
            encryption.gpg_encrypt(filename)
    
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

