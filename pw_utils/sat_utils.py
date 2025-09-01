
# Copyright Aug 2025, Philip Wright. All rights reserved. 

import hashlib
import os
import sys
from pw_utils import mydb_utils

def calc_digest(vdate_loaded,erws_sc, erws_dt, mss_sc, mss_dt, total_sc, total_dt, vlast_name, vfirst_name, vmiddle_name, vbirthdate, vemail_addr, vaddress1, vcity, vstate, vpostal):

    # vdate_loaded yyyy-mm-dd
    # test dates    yyyymm
    # vdob         yyyy-mm-dd
    # vlast_name, vfirst_name length max 30
    # vmiddle name max len 1
    # email len 50 max
    # city max len 30
    # state max len 2
    # postal max len 5
    
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

    if os.environ.get('DISABLE_SCORE_DIGEST', '') != '':
        return ["",""]
    
    digarr = []
    error = False

    if erws_sc is None:
        erws_sc = ""
        erws_dt = ""

    if mss_sc is None:
        mss_sc = ""
        mss_dt = ""

    if total_sc is None:
        total_sc = ""
        total_dt = ""

    
    error = ((erws_sc == '' and len(erws_dt) > 0) or
             (mss_sc == '' and len(mss_dt) > 0) or
             (total_sc == '' and len(total_dt) > 0))
    
    #if error:
     #   mydb_utils.uga_out(sys.stdout, [vdate_loaded, erws_sc, erws_dt, mss_sc, mss_dt, vlast_name, vfirst_name, vmiddle_name, vbirthdate, vemail_addr, vaddress1, vcity, vstate,vpostal])
        #raise Exception("calc_digest score date status error")

    for f in (vdate_loaded,erws_sc, erws_dt, mss_sc, mss_dt, total_sc, total_dt, vlast_name,vfirst_name,vmiddle_name,vbirthdate,vemail_addr,vaddress1,vcity,vstate,vpostal):
        if f != "--" and not f is None:
            digarr.append(f)
        else:
            digarr.append("")
    digparts = ",".join(digarr)
    digparts_enc = digparts.encode('utf-8')
    sha256_hash = hashlib.sha256(digparts_enc)
    hexdigest = sha256_hash.hexdigest()
    return [digparts, hexdigest]

#---------------------------------
def calc_digest_nld(erws_sc, erws_dt, mss_sc, mss_dt, total_sc, total_dt, vlast_name, vfirst_name, vmiddle_name, vbirthdate, vemail_addr, vaddress1, vcity, vstate, vpostal, flog=None, fname=None, line_count=None):

    vlast_name = vlast_name[0:30].upper()
    vfirst_name = vfirst_name[0:30].upper()
    vmiddle_name = vmiddle_name[0:1].upper()
    vbirthdate = vbirthdate[0:10]
    vemail_addr = vemail_addr[0:50].upper()
    vaddress1 = vaddress1[0:40].upper()
    vcity = vcity[0:30].upper()
    vstate = vstate[0:2].upper()
    vpostal = vpostal[0:5]

    if os.environ.get('DISABLE_SCORE_DIGEST', '') != '':
        return ["",""]
    
    digarr = []
    error = False

    if erws_sc is None:
        erws_sc = ""
        erws_dt = ""

    if mss_sc is None:
        mss_sc = ""
        mss_dt = ""

    if total_sc is None:
        total_sc = ""
        total_dt = ""

    
    error = ((erws_sc == '' and len(erws_dt) > 0) or
             (mss_sc == '' and len(mss_dt) > 0) or
             (total_sc == '' and len(total_dt) > 0))
    
    # missing SAT1 scores. May be an old score with SAT2 subject tests only
    #if error:
     #   mydb_utils.uga_out(flog, [fname, line_count, vlast_name, vfirst_name, "","","",f"calc_digest_nld: No Sat1 score data for digest calc. Sat subject scores will not be matched"])
        #raise Exception("calc_digest score date status error")

    for f in (erws_sc, erws_dt, mss_sc, mss_dt, total_sc, total_dt, vlast_name,vfirst_name,vmiddle_name,vbirthdate,vemail_addr,vaddress1,vcity,vstate,vpostal):
        if f != "--" and not f is None:
            digarr.append(f)
        else:
            digarr.append("")
    digparts = ",".join(digarr)
    digparts_enc = digparts.encode('utf-8')
    sha256_hash = hashlib.sha256(digparts_enc)
    hexdigest = sha256_hash.hexdigest()
    return hexdigest


def delete_sat_records(conn, satrecord_ids):
    q4 = "delete from sat_exclude where satrecord_id = ?"
    q3 = "delete from sat_matched_keys where satrecord_id = ?"
    q2 = "delete from satrecord_addl_2 where satrecord_id = ?"
    q1 = "delete from satrecord_addl_1 where satrecord_id = ?"
    q0 = "delete from satrecord where satrecord_id = ?"
    
    for satrecord_id in satrecord_ids:
        for q in [q4,q3,q2,q1,q0]:
            cur = conn.cursor()
            res = cur.execute(q,(satrecord_id,))
            #conn.commit()
    
def delete_sat_file(conn, satfile_id):
    q = """
    select satrecord_id from satrecord
    where satfile_id = ?
    """
    satrecord_ids = []
    cur = conn.cursor()
    for row in cur.execute(q, (satfile_id,)):
        satrecord_id, = row
        satrecord_ids.append(satrecord_id)
    cur.close
    #print(f"would delete sat records {satrecord_ids} for satfile_id {satfile_id}")
    delete_sat_records(conn, satrecord_ids)
    q2 = "delete from satfile where satfile_id = ?"
    cur = conn.cursor()
    cur.execute(q2, (satfile_id,))
    #conn.commit()
