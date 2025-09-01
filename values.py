#!/usr/bin/python3

# Copyright Aug 2025, Philip Wright. All rights reserved.

import sys

# Copyright Aug 2025, Philip Wright. All rights reserved. 

note="""generate the code for ?, list for use in a query insert"""

if len(sys.argv) != 2:
   print("use _ num")
   sys.exit()

arg = sys.argv[1]
int_arg = int(arg)
for i in range(0,int_arg):
   if i == 0:
      print("?", end='')
   else:
      print(",?", end='')
print("")
