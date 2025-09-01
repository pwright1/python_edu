
# Copyright Aug 2025, Philip Wright. All rights reserved. 

import re
def pct_w(string):
    """implement ruby's %w[] functionality"""
    normalized_string = re.sub(r"\s+"," ", string)
    result = normalized_string.split(" ")
    return result

def normalize(string):
    result = re.sub(r"\s","",string)
    return result

# just use .get() instead of this
def hash_lu(hash, string):
    value = None
    try:
        value = hash[string]
        return value
    except Exception as err:
        return value
    return value

def dup_csv_value(value):
    if re.findall(r",", value):
        entries = value.split(",")
        sorted_entries = sorted(entries)
        deduped_entries = sorted(list(set(entries)))
        #for v in sorted_entries:
        #    print(v)
        #for v in deduped_entries:
        #    print(v)
        if len(sorted_entries) > len(deduped_entries):
            return True
    return False

def excel_name_from_csv_name(csv_name):
    if not (len(csv_name) > 5 and (csv_name[-4:].lower() == ".csv" or csv_name[-4:].lower() == ".txt")):
        print(f"bad csv name {csv_name} for excel_name_from_csv_name()")
        return f"{csv_name}.xlsx"
    base_name = csv_name[0:len(csv_name)-4]
    excel_name = f"{base_name}.xlsx"
    return excel_name

def mytype(val):
    ivalue = None
    fvalue = None
    svalue = None
    
    try:
        ivalue = int(val)
    except ValueError:
        pass
    try:
        fvalue = float(val)
    except ValueError:
        pass

    try:
        svalue = str(val)
    except ValueError:
        pass

    if not ivalue is None:
        return ["int", ivalue]
    
    if not fvalue is None:
        return ["float", fvalue]
    
    if not svalue is None:
        return ["str",svalue]
    
    return ["",None]
    
        
        
