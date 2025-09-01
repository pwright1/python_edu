#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import re
import os
import sys



def pct_w(string):
    """simulate ruby's %w functionality"""
    # collapse multiple spaces down to one, newlines...
    normalized_string = re.sub("\s+"," ", string)
    result = normalized_string.split(" ")
    return result

str1 = "A B C D E F G"
str2 = """E
F
G
H
I"""

str1_arr = pct_w(str1)
str2_arr = pct_w(str2)
str3_arr = pct_w("The Quick Brown Fox")

print(str1_arr)
print(str2_arr)
print(str3_arr)

results = """
['A', 'B', 'C', 'D', 'E', 'F', 'G']
['E', 'F', 'G', 'H', 'I']
['The', 'Quick', 'Brown', 'Fox']
"""

print(os.name)
print(sys.platform)

