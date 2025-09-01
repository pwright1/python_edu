#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import hashlib
import traceback
import sqlite3
import os.path
import time
import re
#from datetime import datetime
from datetime import date
from pw_utils import act_match_result
from pw_utils import act_match_src
from pw_utils import act_utils
from pw_utils import applicant_biod
from pw_utils import match
from pw_utils import match_result
from pw_utils import match_util
from pw_utils import mydb_utils
from pw_utils import score_tables_utils
from pw_utils import string_utils


"""
"""

class Bork:
    debug = False
    def __init__(self):
        if self.debug:
            print("class {} init".format(type(self)) )
        self.exclusion_hash = {}

    def update_query_pass(self, conn, scc_temp_id):
        q = "update actstage set query_pass = 1 where scc_temp_id = ?"
        cur = conn.cursor()
        tup = (scc_temp_id,)
        res = cur.execute(q, tup)
        
    def load_exclusions(self, conn):
        if os.environ.get("DISABLE_ACT_EXCL", None) is None:
            qexcl = "select actrecord_id, siss_id, emplid from act_exclude"
            cur = conn.cursor()
            for row in cur.execute(qexcl):
                actrecord_id, siss_id, emplid = row
                accum = f"{actrecord_id}_{siss_id}_{emplid}"
                if string_utils.hash_lu(self.exclusion_hash, accum) is None:
                    self.exclusion_hash[accum] = True
            cur.close()
        else:
            print("Disabling act excl as DISABLE_ACT_EXCL is set")

    def is_exclusion(self, actrecord_id, siss_id, emplid):
        accum = f"{actrecord_id}_{siss_id}_{emplid}"
        lu =  string_utils.hash_lu(self.exclusion_hash, accum)
        if lu is None:
            return False
        if lu == True:
            return True
        
    def go(self, conn, match_pass):
        print(f"go {match_pass}")
        if match_pass == "1":
            self.go1(conn)
        elif match_pass == "2":
            self.go2(conn)

    def go1(self, conn):
        actstage_update_arr = []
        hdr = act_match_result.ActMatchResult.header()
        #pariahFilename = "act_pariah1.txt"
        unmatchedFilename = "act_unmatched1.txt"
        readyFilename = "act_ready1.txt"
        exclFilename = "act_excl.txt"
        #pariahFile = open(pariahFilename, "w", encoding="UTF-8")
        pariahFile = None
        unmatchedFile = open(unmatchedFilename, "w", encoding="UTF-8")
        readyFile = open(readyFilename, "w", encoding="UTF-8")
        exclFile = open(exclFilename, "w", encoding="UTF-8")
        
        #mydb_utils.uga_out(pariahFile,hdr)
        mydb_utils.uga_out(unmatchedFile,hdr)
        mydb_utils.uga_out(readyFile,hdr)
        mydb_utils.uga_out(exclFile,hdr)
    
        act_count_q = """
        select count(*)
        from actstage s, actrecord r
        where s.actrecord_id = r.actrecord_id
        and s.query_pass = 0
        """
        cur = conn.cursor()
        row_count = 0
        for row in cur.execute(act_count_q):
            row_count, = row
        print(f"the count is {row_count}")
        cur.close()
        
        act_data_q = """
        select distinct s.actrecord_id, s.scc_temp_id, s.last_name, s.first_name, s.middle_name,
        r.genderalpha, r.edob, s.address1, s.city, s.state, s.postal,
        r.hscode, s.email_addr,  s.date_loaded, r.actid
        from actstage s, actrecord r
        where s.actrecord_id = r.actrecord_id
        and s.query_pass = 0
        """
        cur = conn.cursor()
        i = 0
        for row in cur.execute(act_data_q):
            actrecord_id, scc_temp_id, last, first, mi, gender, edob, street, city, state, zip5, hscode, email, loaddate, actid = row
            vedob = mydb_utils.iso_to_date(edob)

            #mydb_utils.uga_out(sys.stdout, [actrecord_id, scc_temp_id, last, first, mi, gender, vedob, street, city, state, zip5, hscode, email])

            actMatchSrc = act_match_src.ActMatchSrc(actrecord_id, scc_temp_id, last, first, mi, gender, vedob, street, city, state, zip5, hscode, email, loaddate, actid)

            self.do_match1(conn, actMatchSrc, unmatchedFile, readyFile, pariahFile, actstage_update_arr)
            if (i % 100) == 0:
                pct = (100 * i / row_count)
                print(f"M1: i {i} pct {pct:.0f}")
            
            actMatchSrc = None
            i += 1
            
            
        #pariahFile.close()
        unmatchedFile.close()
        readyFile.close()
        exclFile.close()
        #print(f"pass1 actstage_update_arr count {len(actstage_update_arr)}")
        for i, scc_temp_id in enumerate(actstage_update_arr):
            self.update_query_pass(conn, scc_temp_id)
            if i % 10 == 0:
                print(i)
                conn.commit()
        conn.commit()
        
    #-------------------------------------------------
    def do_match1(self, conn, actMatchSrc, unmatchedFile, readyFile, pariahFile, update_arr):
        mat = match.Match()
        mu = match_util.MatchUtil()
        qfast = """
        select distinct slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, cellphone, homephone, fatherphone, motherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, mpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, hpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, spostal, scountry, orgid, ceeb, test_consider from applicant_biod
        where test_consider in ('', '1')
        and
        (
        (upper(last) = ?  and upper(substr(first,1,1)) = ?) or
        (upper(last) = ?  and upper(substr(first,1,1)) = ?) or
        (upper(first) = ? and upper(last) like ?) or   
        (upper(first) = ? and upper(last) like ?) or
        (ceeb = ? and ceeb <> '' )
        )
        and
        (
        (birthdate = ?)  or
        (upper(substr(mpostal,1,5)) = ? and mpostal <> '') or
        (upper(substr(hpostal,1,5)) = ? and hpostal <> '') or
        (upper(mcity) = ? and mcity <> '') or
        (upper(hcity) = ? and hcity <> '') or
        (upper(scity) = ? and scity <> '') or
        (ceeb = ? and ceeb <> '') or
        (upper(email) = ? and email <> '')
        )
        """
        paras = []
        paras.append(string_utils.normalize(actMatchSrc.last.upper()))
        paras.append(actMatchSrc.first[0:1].upper())
        paras.append(string_utils.normalize(actMatchSrc.last.upper()))
        paras.append(actMatchSrc.mi[0:1].upper())
        paras.append(string_utils.normalize(actMatchSrc.last.upper()))
        paras.append(actMatchSrc.first[0:1].upper() + '%')
        paras.append(string_utils.normalize(actMatchSrc.last.upper()))
        paras.append(actMatchSrc.mi[0:1].upper() + '%')
        paras.append(actMatchSrc.hscode)
        paras.append(actMatchSrc.edob)
        paras.append(actMatchSrc.zip5.upper())
        paras.append(actMatchSrc.zip5.upper())
        paras.append(actMatchSrc.city.upper())
        paras.append(actMatchSrc.city.upper())
        paras.append(actMatchSrc.city.upper())
        paras.append(actMatchSrc.hscode)
        paras.append(actMatchSrc.email.upper())
        tparas = tuple(paras)
        #print(tparas)
        cur = conn.cursor()
        for row in cur.execute(qfast, tparas):
            slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, cellphone, homephone, fatherphone, motherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, mpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, hpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, spostal, scountry, orgid, ceeb, test_consider = row
            
            vhomephone = mu.phone_cleanup(homephone)
            vcellphone = mu.phone_cleanup(cellphone)
            vmotherphone = mu.phone_cleanup(motherphone)
            vfatherphone = mu.phone_cleanup(fatherphone)

            vmpostal = re.sub(r"\-","",mpostal)
            vhpostal = re.sub(r"\-","",hpostal)
            vspostal = re.sub(r"\-","",spostal)
            
            applicantBiod = applicant_biod.ApplicantBiod(slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, vcellphone, vhomephone, vfatherphone, vmotherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, vmpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, vhpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, vspostal, scountry, orgid, ceeb, test_consider)

            appHomeBiodemo = applicantBiod.homeBiodemo()
            appMailBiodemo = applicantBiod.mailBiodemo()
            actBiodemo = actMatchSrc.biodemo()
            homeMat = mat.compare1(actBiodemo, appHomeBiodemo)
            homeRat = homeMat.maRating
            mailMat = mat.compare1(actBiodemo, appMailBiodemo)
            mailRat = mailMat.maRating
            maxMat = match_result.MatchResult()
            maxRat = 0
            if homeRat >= mailRat:
                maxMat = homeMat
                maxRat = homeRat
            else:
                maxMat = mailMat
                maxRat = mailRat

            if not self.is_exclusion(actMatchSrc.actrecord_id, actMatchSrc.scc_temp_id, applicantBiod.emplid):
                if maxRat > 7:
                    if maxMat.isAutoMatch():
                        readyRes = act_match_result.ActMatchResult(applicantBiod,actMatchSrc, maxMat)
                        mydb_utils.uga_out(readyFile, readyRes.toArr())
                    else:
                        unmatchedRes = act_match_result.ActMatchResult(applicantBiod,actMatchSrc, maxMat)
                        mydb_utils.uga_out(unmatchedFile, unmatchedRes.toArr())
                    update_arr.append(actMatchSrc.scc_temp_id)
                else:
                    pass
                    #pariahRes = act_match_result.ActMatchResult(applicantBiod,actMatchSrc, maxMat)
                    #mydb_utils.uga_out(pariahFile, pariahRes.toArr())
        pass # do_match1()

    #-------------------------------------------------
    def go2(self, conn):
        hdr = act_match_result.ActMatchResult.header()

        #pariahFilename = "act_pariah2.txt"
        unmatchedFilename = "act_unmatched2.txt"
        readyFilename = "act_ready2.txt"

        #pariahFile = open(pariahFilename, "w", encoding="UTF-8")
        pariahFile = None
        unmatchedFile = open(unmatchedFilename, "w", encoding="UTF-8")
        readyFile = open(readyFilename, "w", encoding="UTF-8")
        
        #mydb_utils.uga_out(pariahFile,hdr)
        mydb_utils.uga_out(unmatchedFile,hdr)
        mydb_utils.uga_out(readyFile,hdr)
    
        act_count_q = """
        select count(*)
        from actstage s, actrecord r
        where s.actrecord_id = r.actrecord_id
        and s.query_pass = 0
        """
        cur = conn.cursor()
        row_count = 0
        for row in cur.execute(act_count_q):
            row_count, = row
        print(f"the count is {row_count}")
        cur.close()
        
        act_data_q = """
        select distinct s.actrecord_id, s.scc_temp_id, r.last, r.first, r.mi,
        r.genderalpha, r.edob, r.street, r.city, r.state, r.zip5,
        r.hscode, s.email_addr,  s.date_loaded, r.actid
        from actstage s, actrecord r
        where s.actrecord_id = r.actrecord_id
        and s.query_pass = 0
        """
        cur = conn.cursor()
        i = 0
        for row in cur.execute(act_data_q):
            actrecord_id, scc_temp_id, last, first, mi, gender, edob, street, city, state, zip5, hscode, email, loaddate, actid = row
            vedob = mydb_utils.iso_to_date(edob)

            #7/27/25 uncommented bork
            #mydb_utils.uga_out(sys.stdout, [actrecord_id, scc_temp_id, last, first, mi, gender, vedob, street, city, state, zip5, hscode, email])

            actMatchSrc = act_match_src.ActMatchSrc(actrecord_id, scc_temp_id, last, first, mi, gender, vedob, street, city, state, zip5, hscode, email, loaddate, actid)

            self.do_match2(conn, actMatchSrc, unmatchedFile, readyFile, pariahFile)
            if (i % 100) == 0:
                pct = (100 * i / row_count)
                print(f"M2: i {i} pct {pct:.0f}")
            
            actMatchSrc = None
            i += 1
            
        #pariahFile.close()
        unmatchedFile.close()
        readyFile.close()

    #-----------------------------------------------
    def do_match2(self, conn, actMatchSrc, unmatchedFile, readyFile, pariahFile):
        mat = match.Match()
        mu = match_util.MatchUtil()
        qrest = """
        select distinct slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, cellphone, homephone, fatherphone, motherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, mpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, hpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, spostal, scountry, orgid, ceeb, test_consider from applicant_biod
        where test_consider in ('', '1')
        and
        (upper(email) = ? and email <> '') or
        (
        ((ceeb = ? and ceeb <> '') or
        (birthdate = ? and birthdate <> '')  or
        (upper(substr(mpostal,1,5)) = ? and mpostal <> '') or
        (upper(substr(hpostal,1,5)) = ? and hpostal <> '')) and
        
        (upper(last) <> ?) and

        ((upper(mcity) = ? and mcity <> '') or
        (upper(hcity) = ? and hcity <> '') or
        (upper(scity) = ? and scity <> '') or
        (upper(substr(first,1,1)) = ? ) or
        (upper(substr(first,1,1)) = ?))
        )
        """
        paras = []
        paras.append(actMatchSrc.email.upper())
        paras.append(actMatchSrc.hscode)
        paras.append(actMatchSrc.edob)
        paras.append(actMatchSrc.zip5.upper())
        paras.append(actMatchSrc.zip5.upper())
        paras.append(string_utils.normalize(actMatchSrc.last.upper()))
        paras.append(actMatchSrc.city.upper())
        paras.append(actMatchSrc.city.upper())
        paras.append(actMatchSrc.city.upper())
        paras.append(actMatchSrc.first[0:1].upper())
        paras.append(actMatchSrc.mi[0:1].upper())
        tparas = tuple(paras)
        cur = conn.cursor()
        for row in cur.execute(qrest, tparas):
            slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, cellphone, homephone, fatherphone, motherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, mpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, hpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, spostal, scountry, orgid, ceeb, test_consider = row

            vhomephone = mu.phone_cleanup(homephone)
            vcellphone = mu.phone_cleanup(cellphone)
            vmotherphone = mu.phone_cleanup(motherphone)
            vfatherphone = mu.phone_cleanup(fatherphone)
            
            vmpostal = re.sub(r"\-","",mpostal)
            vhpostal = re.sub(r"\-","",hpostal)
            vspostal = re.sub(r"\-","",spostal)
            
            applicantBiod = applicant_biod.ApplicantBiod(slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, vcellphone, vhomephone, vfatherphone, vmotherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, vmpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, vhpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, vspostal, scountry, orgid, ceeb, test_consider)
            
            appHomeBiodemo = applicantBiod.homeBiodemo()
            appMailBiodemo = applicantBiod.mailBiodemo()
            actBiodemo = actMatchSrc.biodemo()
            homeMat = mat.compare1(actBiodemo, appHomeBiodemo)
            homeRat = homeMat.maRating
            mailMat = mat.compare1(actBiodemo, appMailBiodemo)
            mailRat = mailMat.maRating
            maxMat = match_result.MatchResult()
            maxRat = 0
            if homeRat >= mailRat:
                maxMat = homeMat
                maxRat = homeRat
            else:
                maxMat = mailMat
                maxRat = mailRat

            if not self.is_exclusion(actMatchSrc.actrecord_id, actMatchSrc.scc_temp_id, applicantBiod.emplid):
                if maxRat > 7:
                    if maxMat.isAutoMatch():
                        readyRes = act_match_result.ActMatchResult(applicantBiod,actMatchSrc, maxMat)
                        mydb_utils.uga_out(readyFile, readyRes.toArr())
                    else:
                        unmatchedRes = act_match_result.ActMatchResult(applicantBiod,actMatchSrc, maxMat)
                        mydb_utils.uga_out(unmatchedFile, unmatchedRes.toArr())
                else:
                    pass
                    #pariahRes = act_match_result.ActMatchResult(applicantBiod,actMatchSrc, maxMat)
                    #mydb_utils.uga_out(pariahFile, pariahRes.toArr())
        pass # do_match2()
    
        
            
def main():
    conn = None
    try:
        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        scores_db_file = os.path.join(db_dir, scores_db_name)
        
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")
        #print(f"scores db file {scores_db_file} not found. will be created")
        
        conn = mydb_utils.sqlite3_connect(scores_db_file)
        b = Bork()

        if len(sys.argv[1:]) != 1:
            print("use act_match.py  1 | 2")
            sys.exit(-1)
            
        match_pass = sys.argv[1]
        if not match_pass in ["1","2"]:
            print("use act_match.py  1 | 2")
            sys.exit(-1)

        b.load_exclusions(conn)
        b.go(conn, match_pass)
            
        fout = None
    except Exception as err:
        print("here is your exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
        
    finally:
        if not conn is None:
            conn.close()
        pass
main()

