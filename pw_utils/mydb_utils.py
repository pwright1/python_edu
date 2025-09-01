
# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import os
import platform
from datetime import datetime
from pw_utils import string_utils
from pw_utils import mydb_utils
import sqlite3
import csv
import xlsxwriter
import json
import time
import re
from glob import glob

def on_windows():
    system = platform.system()
    if system == "Windows":
        return True
    return False

def python_name():
    if on_windows():
        return "python"
    else:
        return "python3"

def check_env_vars():
    #var_entries = string_utils.pct_w("SQLITE3_DB_DIR PYTHON_SCRIPT_DIR PD RHUBARB UGA_DIR SEMAPHORE")
    var_entries = string_utils.pct_w("SQLITE3_DB_DIR PYTHON_SCRIPT_DIR PY UGA_DIR SEMAPHORE")
    for entry in var_entries:
        ventry = f"{entry}:"
        value = os.environ.get(entry, "Unset")
        a_file = False
        a_dir = False
        if value != "Unset":
            a_file = os.path.isfile(value)
            a_dir = os.path.isdir(value)
        
        print(f"{ventry:<20}value: {value:<50} file? {a_file:<5} dir? {a_dir:<5}")
    # check for .sqliterc in HOME directory
    sqlite_rc_file = ".sqliterc"
    homedir = os.environ.get("HOME","Unset")
    if homedir == "Unset":
        print("HOME ENV VAR NOT SET, can't check for .sqliterc")
    else:
        sqlite_rc_file_fullpath = os.path.join(homedir, sqlite_rc_file)
        if not os.path.exists(sqlite_rc_file_fullpath):
            print(f"could not find file {sqlite_rc_file_fullpath}")
        else:
            print(f".sqliterc found at {sqlite_rc_file_fullpath}")
    
    

def get_sqlite3_db_dir():
    db_dir = os.environ.get('SQLITE3_DB_DIR', None)
    if db_dir is None:
        msg = "get_sqlite3_db_dir returns None for SQLITE3_DB_DIR env var"
        raise RuntimeError(msg)
    else:
        return db_dir

def get_python_script_dir():
    script_dir = os.environ.get('PYTHON_SCRIPT_DIR', None)
    if script_dir is None:
        msg = "get_python_script_dir() returns none for PYTHON_SCRIPT_DIR env var"
        raise RuntimeError(msg)
    else:
        return script_dir

def sqlite3_connect(the_file):
    # need to connect with this functon so the pragma gets set
    if not os.path.exists(the_file):
        raise RuntimeError(f"Database file {the_file} does not exist or is encrypted")

    conn = sqlite3.connect(the_file)
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = on")
    return conn

def sqlite3_attach(conn, the_file, schema):
    if not os.path.exists(the_file):
        raise RuntimeError(f"Database file {the_file} does not exist or is encrypted")
    cur = conn.cursor()
    cur.execute(f"attach database {the_file} as {schema}")

def sqlite3_detatch(conn, schema):
    cur = conn.cursor()
    cur.execute(f"detatch database {schema}")

def split_sq_csv_line(line):
    delim = "\",\""
    fields = line.split(delim)
    nfields = len(fields)
    startfield = fields[0]
    fields[0] = startfield[1:]
    endfield = fields[nfields-1]
    fields[nfields-1] = endfield[0:-1]
    return fields

def get_file_ts():
    tstamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return tstamp

def get_db_ts():
    tstamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return tstamp

def get_ays_ts():
    tstamp = datetime.now().strftime("%m/%d/%Y")
    return tstamp

def uga_out(fh, arr):
    errors = 0
    msg = ""
    count = len(arr)
    #print("mydb_utils.uga_out fields {}".format(count))
    for i in range(0,count):
        if arr[i] == None:
            msg = "uga_out None field at column {}".format(i)
            print(msg)
            arr[i] = ""
            errors += 1
        if i == 0:
            fh.write("\"")
            fh.write("{0:s}".format(str(arr[i])))
        else:
            fh.write("\",\"")
            fh.write("{0:s}".format(str(arr[i])))
    fh.write("\"\n")
    return [errors,msg]

def uga_out_noquote(fh, arr):
    errors = 0
    msg = ""
    count = len(arr)
    #print("mydb_utils.uga_out fields {}".format(count))
    for i in range(0,count):
        if arr[i] == None:
            msg = "uga_out None field at column {}".format(i)
            print(msg)
            arr[i] = ""
            errors += 1
        if i == 0:
            fh.write("{0:s}".format(str(arr[i])))
        else:
            fh.write(",")
            fh.write("{0:s}".format(str(arr[i])))
    fh.write("\n")
    return [errors,msg]
                                                                        
def uga_out_pipe(fh, arr):
    errors = 0
    msg = ""
    count = len(arr)
    #print("mydb_utils.uga_out fields {}".format(count))
    for i in range(0,count):
        if arr[i] == None:
            msg = "uga_out None field at column {}".format(i)
            print(msg)
            arr[i] = ""
            errors += 1
        if i == 0:
            fh.write("\"")
            fh.write("{0:s}".format(str(arr[i])))
        else:
            fh.write("\"|\"")
            fh.write("{0:s}".format(str(arr[i])))
    fh.write("\"\n")         
    return [errors,msg]



def to_crockford32(num):
    arr = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    accum = ""
    if num == 0:
        return "0"
    while num > 0:
        div = int(num / 32)
        rem = int((num % 32))
        num = div
        #print("{} {} {}".format(num, div, rem))
        accum = arr[rem] + accum
    return accum

def psdate_to_iso(date):
    yyyy = date[0:4]
    mm = date[4:6]
    dd = date[6:8]
    return f"{yyyy}-{mm}-{dd}"

def date_to_iso(date):
    mm = date[0:2]
    dd = date[3:5]
    yyyy = date[6:10]
    return f"{yyyy}-{mm}-{dd}"

def iso_to_date(date):
    yyyy = date[0:4]
    mm = date[5:7]
    dd = date[8:10]
    return f"{mm}/{dd}/{yyyy}"

#def create_excel(fileName):
#    # for windows needs to be a WSL call
#    cmd = ""
#    if on_windows():
#        cmd = "wsl /opt/excel/ExcelSend.rb #{fileName} 2>/dev/null 1>/dev/null"
#    else:
#        cmd = "/opt/excel/ExcelSend.rb #{fileName} 2>/dev/null 1>/dev/null"
#    os.system(cmd)

def blank_string_array(cols):
    arr = []
    for i in range(cols):
        arr.append("")
    return arr

def csv_to_xlsx(fname_csv,dialect="", json_fname=""):

    #column width / type info if present in separate json file
    json_arr = []
    json_arr_len = 0
    if json_fname != "":
        with open(json_fname, "r", encoding='UTF-8') as json_fin:
            obj = json.load(json_fin)
            if 'carray' in obj:
                json_arr = obj['carray']
    json_arr_len = len(json_arr)
    #print(json_arr)


    with open(fname_csv, "r") as fin:
        reader = None
        if dialect == "excel":
            reader = csv.reader(fin, dialect='excel')
        else:
            reader = csv.reader(fin, delimiter=',', quotechar='"')

        excel_name = string_utils.excel_name_from_csv_name(fname_csv)
        workbook = xlsxwriter.Workbook(excel_name)
        worksheet = workbook.add_worksheet()
        text_format = workbook.add_format({'num_format': '@'})
        # doesn't work...
        general_format = workbook.add_format({'num_format': '0'})
        
        if json_arr_len > 0:
            for i, the_dict in enumerate(json_arr):
                vwidth = None
                vformat = text_format
                width = the_dict.get('width',None)
                if width is None:
                    vwidth = None
                else:
                    vwidth = float(width)
                    if abs(vwidth - 0.0) < 0.1:
                        vwidth = None
                ctype = the_dict.get('cellType',None)
                if ctype is None:
                    vformat = text_format
                    worksheet.set_column(i,i,vwidth, vformat)
                elif ctype == 0:
                    vformat = general_format
                    #worksheet.set_column(i,i,vwidth, vformat)
                    worksheet.set_column(i,i,vwidth)
                elif ctype == 1:
                    vformat = text_format
                    worksheet.set_column(i,i,vwidth, vformat)
                #print(f"setting column {i} {vwidth} {vformat}")
                    
        line_number = 0
        for row in reader:
            num_cols = len(row)
            if json_arr_len > 0 and num_cols != json_arr_len:
                print(f"json_arr_len != excel_num_columns")
            for j in range(0, num_cols):
                vtype, vvalue = string_utils.mytype(row[j])
                # if we have a json column settings file and not the header row
                if len(json_arr) > 0 and line_number > 0:
                    ctype = json_arr[j].get('cellType', None)
                    # unset or string type
                    if ctype is None or ctype == 1:
                        worksheet.write(line_number, j, str(row[j]))
                    # numeric type requested. write as number if a valid number
                    elif ctype == 0:
                        if vtype == "int" or vtype == "float":
                            #worksheet.write(line_number, j, vvalue, general_format)
                            worksheet.write(line_number, j, vvalue)
                        # write it as a string
                        else:
                            worksheet.write(line_number, j, str(row[j]),text_format)
                else:
                    worksheet.write(line_number, j, str(row[j]), text_format)
            line_number += 1
    workbook.close()
    time.sleep(1)
    if os.path.isfile(excel_name) and os.path.isfile(fname_csv):
        os.remove(fname_csv)
    
def get_glob_args():
    result_args = []
    args = sys.argv[1:]
    for arg in args:
        if on_windows():
            if re.findall(r"\*",arg):
                result_args.extend(sorted(glob(arg)))
            else:
                result_args.append(arg)
        else:
            result_args.append(arg)
    return result_args
