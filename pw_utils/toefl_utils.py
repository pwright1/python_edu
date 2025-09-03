
# Copyright Sep 2025, Philip Wright. All rights reserved. 

import hashlib
import os
import sys
from pw_utils import mydb_utils


def calc_digest_nld(ran, vlast_name, vfirst_name, vmiddle_name, vbirthdate, vemail_addr, vcity, vcountry):
    """Calc the sha256 hex digest for the values. It represents a signature of the data. if any data item changes, so does the signature. Used to match data stored in separate systems that limit access to fields directly. nld: no load date"""
    
    vlast_name = vlast_name[0:30].upper()
    vfirst_name = vfirst_name[0:30].upper()
    vmiddle_name = vmiddle_name[0:1].upper()
    vbirthdate = vbirthdate[0:10]
    vemail_addr = vemail_addr[0:50].upper()
    #vaddress1 = vaddress1[0:40].upper()
    vcity = vcity[0:30].upper()
    digarr = []

    for f in (ran, vlast_name, vfirst_name, vmiddle_name, vbirthdate, vemail_addr, vcity, vcountry):
        if f != "--" and not f is None:
            digarr.append(f)
        else:
            digarr.append("")
    digparts = "^".join(digarr)
    digparts_enc = digparts.encode('utf-8')
    sha256_hash = hashlib.sha256(digparts_enc)
    hexdigest = sha256_hash.hexdigest()
    return [digparts, hexdigest]


def delete_toefl_records(conn, toefl_record_ids):
    q4 = "delete from toefl_exclude where toefl_record_id = ?"
    q3 = "delete from toefl_matched_keys where toefl_record_id = ?"
    q0 = "delete from toefl_record where toefl_record_id = ?"
    
    for satrecord_id in satrecord_ids:
        for q in [q4,q3,q2,q1,q0]:
            cur = conn.cursor()
            res = cur.execute(q,(satrecord_id,))
            #conn.commit()
    
def delete_toefl_file(conn, toefl_file_id):
    q = """
    select toefl_record_id from toefl_record
    where toefl_file_id = ?
    """
    toefl_record_ids = []
    cur = conn.cursor()
    for row in cur.execute(q, (toefl_file_id,)):
        toefl_record_id, = row
        toefl_record_ids.append(toefl_record_id)
    cur.close
    #print(f"would delete toefl records {toefl_record_ids} for toefl_file_id {toefl_file_id}")
    delete_toefl_records(conn, toefl_record_ids)
    q2 = "delete from toefl_file where toefl_file_id = ?"
    cur = conn.cursor()
    cur.execute(q2, (toefl_file_id,))
    #conn.commit()
