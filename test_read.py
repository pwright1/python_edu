#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
class Bork:
    debug = False
    
    def __init__(self):
        pass
    
    def go(self):
        if len(sys.argv[1:]) < 1:
            print("use _ filename(s).txt")
            sys.exit(-1)
        for fname in sys.argv[1:]:
            with open(fname, "r", encoding="UTF-8") as file:
                done = False
                linecount = 1
                while not done:
                    line = file.readline()
                    print("{} lineno {} length {}".format(fname,linecount, len(line)))
                    if len(line) == 0:
                        done = True
                        continue
                    linecount += 1
    
        

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
