#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import getpass
import traceback
from pw_utils import encryption

class Bork:
    debug = False
    def __init__(self):
        pass

    def go(self):
        if len(sys.argv[1:]) == 0:
            print("use _ file")
            sys.exit()
        efilename = sys.argv[1]
        the_pass = getpass.getpass("password:")
        encryption.gpg_decrypt(efilename, the_pass)
        
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

