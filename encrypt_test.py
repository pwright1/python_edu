#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import os
import traceback
#from filelock import FileLock, Timeout
#from pw_utils import mydb_utils
from pw_utils import encryption


class Bork:
    debug = False
    def __init__(self):
        pass

    def go(self):
        if len(sys.argv[1:]) == 0:
            print("use _ file")
            sys.exit()
        filename = sys.argv[1]
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

