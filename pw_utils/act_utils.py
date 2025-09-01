
# Copyright Aug 2025, Philip Wright. All rights reserved. 
import hashlib
import os
import sys
import sqlite3
from pw_utils import mydb_utils

def rename_act_file(origname):
    msg = ""
    code = 0
    suffix = origname[-4:]
    base = origname[0:-4]
    if (suffix.lower() != ".csv" and suffix.lower() != ".txt"):
        msg = f"unexpected suffix {suffix}"
        code = -1
        return ["",code, msg]

    if suffix == ".csv":
        if base[0:20] == "ACT-DUKE-UNIVERSITY-":
            remainder = base[20:]
            datestr, numstr = remainder.split("-")
            yyyy = datestr[4:8]
            mm = datestr[0:2]
            dd = datestr[2:4]
            newname = f"act_{yyyy}{mm}{dd}_{numstr}.csv"
            return [newname, code, msg]
        else:
            msg = "unknown csv file name pattern {origname}"
            code = -2
            return ["",code,msg]
    # .txt    
    else:
        if base[0:4] == "ACT_" and base[12:14] == "_C":
            datestr = base[4:12]
            numstr = base[13:]
            newname = f"act_{datestr}_{numstr.lower()}.txt"
            return [newname, code, msg]
        elif (base[0:20] == "ACT-DUKE UNIVERSITY-") or (base[0:20] == "ACT-DUKE-UNIVERSITY-"):
            datestr = base[20:28]
            numstr = base[29:]
            yyyy = datestr[4:8]
            mm = datestr[0:2]
            dd = datestr[2:4]
            newname = f"act_{yyyy}{mm}{dd}_{numstr}.txt"
            return [newname, code, msg]
        else:
            return ["",-3,f"unknown .txt naming for file {origname}"]
    
    return ["",-4,f"unexpected code path for rename_act_file {origname}"]
        
def score_date_check(score, date):
    if score == '' or score == '--' or score is None:
        date = ''
        score = ''
    return [score, date]

def aydate_to_act_dt(mm_dd_yyyy):
    mm = mm_dd_yyyy[0:2]
    dd = mm_dd_yyyy[3:5]
    yyyy = mm_dd_yyyy[6:10]
    return f"{yyyy}{mm}"
    
def mmyyyy_swap(mmyyyy):
    if mmyyyy == '' or mmyyyy is None:
        return ''
    yyyy = mmyyyy[2:6]
    mm = mmyyyy[0:2]
    yyyymm = f"{yyyy}{mm}"
    return yyyymm

def calc_writing(wd1, wd2, wd3, wd4):
    if wd1 == "--" or wd2 == "--" or wd3 == "--" or wd4 == "--":
        return "--"
    if wd1 == "" or wd2 == "" or wd3 == "" or wd4 == "":
        return ""
    sum = int(wd1) + int(wd2) + int(wd3) + int(wd4)
    the_avg = (sum*1.0 / 4.0)
    rounded_average = round(the_avg,0)
    ravg_s = f"{rounded_average:02.0f}"
    return ravg_s

def calc_digest(vdate_loaded,engl,engl_dt,engls,engls_dt,math,math_dt,maths,maths_dt,read,read_dt,reads,reads_dt,scire,scire_dt,sciss,sciss_dt,comp,comp_dt,comps,vlast_name,vfirst_name,vmiddle_name,vbirthdate,vemail_addr,vaddress1,vcity,vstate,vpostal):

    # vdate_loaded yyyy-mm-dd
    # test dates    yyyymm
    # vdob         yyyy-mm-dd
    # engl,math,read,scire,comp  2 digit integer with no decimal point or blank. no '--'
    # vlast_name, vfirst_name length max 30
    # vmiddle name max len 1
    # email len 50 max
    # city max len 30
    # state max len 2
    # postal max len 5
    
    # show date formats used in the incoming data:
    #mydb_utils.uga_out(sys.stdout, [engl_dt, engls_dt, math_dt, maths_dt, read_dt, reads_dt, scire_dt, sciss_dt, comp_dt, comps_dt])
    
    vdate_loaded = vdate_loaded[0:10]
    vlast_name = vlast_name[0:30].upper()
    vfirst_name = vfirst_name[0:30].upper()
    vmiddle_name = vmiddle_name[0:1].upper()
    vbirthdate = vbirthdate[0:10]
    vemail_addr = vemail_addr[0:50].upper()
    vaddress1 = vaddress1[0:40].upper()
    vcity = vcity[0:30].upper()
    vstate = vstate[0:2].upper()
    vpostal = vpostal[0:5]

    if vstate == "FN" or vstate == "CN":
        vstate = ""
    
    if os.environ.get('DISABLE_SCORE_DIGEST', '') != '':
        return ["",""]
    
    digarr = []
    error = False

    if engls is None:
        engls = ""
        engls_dt = ""
        
    if maths is None:
        maths = ""
        maths_dt = ""
        
    if reads is None:
        reads = ""
        reads_dt = ""
        
    if sciss is None:
        sciss = ""
        sciss_dt = ""
        
        
    error = (( (engl == '' or engl == '--')   and len(engl_dt) > 0) or
             ( (engls == '' or engls == '--') and len(engls_dt) > 0) or
             ( (math == '' or math == '--')   and len(math_dt) > 0) or
             ( (maths == '' or math == '--')  and len(maths_dt) > 0) or
             ( (read == '' or read == '--')   and len(read_dt) > 0) or
             ( (reads == '' or reads == '--') and len(reads_dt) > 0) or
             ( (scire == '' or scire == '--') and len(scire_dt) > 0) or
             ( (sciss == '' or sciss == '--') and len(sciss_dt) > 0) or
             ( (comp == '' or comp == '--')  and len(comp_dt) > 0))
    
    if error:
        mydb_utils.uga_out(sys.stdout, [vdate_loaded,engl,engl_dt,engls,engls_dt,math,math_dt,maths,maths_dt,read,read_dt,reads,reads_dt,scire,scire_dt,sciss,sciss_dt,comp,comp_dt,comps,vlast_name,vfirst_name,vmiddle_name,vbirthdate,vemail_addr,vaddress1,vcity,vstate,vpostal])
        #raise Exception("calc_digest score date status error")

    
    for f in (vdate_loaded,engl,engl_dt,engls,engls_dt,math,math_dt,maths,maths_dt,read,read_dt,reads,reads_dt,scire,scire_dt,sciss,sciss_dt,comp,comp_dt,comps,vlast_name, vfirst_name,vmiddle_name,vbirthdate,vemail_addr,vaddress1,vcity,vstate,vpostal):
        if f != "--" and not f is None:
            digarr.append(f)
        else:
            digarr.append("")
    digparts = ",".join(digarr)
    digparts_enc = digparts.encode('utf-8')
    sha256_hash = hashlib.sha256(digparts_enc)
    hexdigest = sha256_hash.hexdigest()
    return [digparts, hexdigest]

def calc_digest_nld(engl,engl_dt,engls,engls_dt,math,math_dt,maths,maths_dt,read,read_dt,reads,reads_dt,scire,scire_dt,sciss,sciss_dt,comp,comp_dt,comps,vlast_name,vfirst_name,vmiddle_name,vbirthdate,vemail_addr,vaddress1,vcity,vstate,vpostal):

    vlast_name = vlast_name[0:30].upper()
    vfirst_name = vfirst_name[0:30].upper()
    vmiddle_name = vmiddle_name[0:1].upper()
    vbirthdate = vbirthdate[0:10]
    vemail_addr = vemail_addr[0:50].upper()
    vaddress1 = vaddress1[0:40].upper()
    vcity = vcity[0:30].upper()
    vstate = vstate[0:2].upper()
    vpostal = vpostal[0:5]

    if vstate == "FN" or vstate == "CN":
        vstate = ""
    
    digarr = []
    error = False

    if engls is None:
        engls = ""
        engls_dt = ""
        
    if maths is None:
        maths = ""
        maths_dt = ""
        
    if reads is None:
        reads = ""
        reads_dt = ""
        
    if sciss is None:
        sciss = ""
        sciss_dt = ""
        
        
    error = (( (engl == '' or engl == '--')   and len(engl_dt) > 0) or
             ( (engls == '' or engls == '--') and len(engls_dt) > 0) or
             ( (math == '' or math == '--')   and len(math_dt) > 0) or
             ( (maths == '' or math == '--')  and len(maths_dt) > 0) or
             ( (read == '' or read == '--')   and len(read_dt) > 0) or
             ( (reads == '' or reads == '--') and len(reads_dt) > 0) or
             ( (scire == '' or scire == '--') and len(scire_dt) > 0) or
             ( (sciss == '' or sciss == '--') and len(sciss_dt) > 0) or
             ( (comp == '' or comp == '--')  and len(comp_dt) > 0))
    
    if error:
        mydb_utils.uga_out(sys.stdout, [engl,engl_dt,engls,engls_dt,math,math_dt,maths,maths_dt,read,read_dt,reads,reads_dt,scire,scire_dt,sciss,sciss_dt,comp,comp_dt,comps,vlast_name,vfirst_name,vmiddle_name,vbirthdate,vemail_addr,vaddress1,vcity,vstate,vpostal])
        #raise Exception("calc_digest score date status error")

    
    for f in (engl,engl_dt,engls,engls_dt,math,math_dt,maths,maths_dt,read,read_dt,reads,reads_dt,scire,scire_dt,sciss,sciss_dt,comp,comp_dt,comps,vlast_name, vfirst_name,vmiddle_name,vbirthdate,vemail_addr,vaddress1,vcity,vstate,vpostal):
        if f != "--" and not f is None:
            digarr.append(f)
        else:
            digarr.append("")
    digparts = ",".join(digarr)
    digparts_enc = digparts.encode('utf-8')
    sha256_hash = hashlib.sha256(digparts_enc)
    hexdigest = sha256_hash.hexdigest()
    return hexdigest



def delete_act_records(conn, actrecord_ids):
      q7 = "delete from act_exclude where actrecord_id = ?"
      q6 = "delete from act_matched_keys where actrecord_id = ?"
      q5 = "delete from actrecord_addl_5 where actrecord_id = ?"
      q4 = "delete from actrecord_addl_4 where actrecord_id = ?"
      q3 = "delete from actrecord_addl_3 where actrecord_id = ?"
      q2 = "delete from actrecord_addl_2 where actrecord_id = ?"
      q1 = "delete from actrecord_addl_1 where actrecord_id = ?"
      q0 = "delete from actrecord where actrecord_id = ?"
    
      for actrecord_id in actrecord_ids:
          for q in [q7,q6,q5,q4,q3,q2,q1,q0]:
              cur = conn.cursor()
              res = cur.execute(q,(actrecord_id,))
              #conn.commit()
    
def delete_act_file(conn, actfile_id):
    q = """
    select actrecord_id from actrecord
    where actfile_id = ?
    """
    actrecord_ids = []
    cur = conn.cursor()
    for row in cur.execute(q, (actfile_id,)):
        actrecord_id, = row
        actrecord_ids.append(actrecord_id)
    cur.close
    #print(f"would delete act records {actrecord_ids} for actfile_id {actfile_id}")
    delete_act_records(conn, actrecord_ids)
    q2 = "delete from actfile where actfile_id = ?"
    cur = conn.cursor()
    cur.execute(q2, (actfile_id,))
    #conn.commit()
