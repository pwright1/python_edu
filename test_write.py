#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
from pw_utils import mydb_utils

class Bork:
    debug = False
    
    def __init__(self):
        pass
    
    def go(self):
        fname = "/tmp/testfile.csv"
        with open(fname, "w", encoding="UTF-8") as file:
            for i in range(0,10):
                mydb_utils.uga_out(file, [i])
        
    
            
        

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
