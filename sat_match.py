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
from pw_utils import sat_match_result
from pw_utils import sat_match_src
from pw_utils import sat_utils
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
        q = "update satstage set query_pass = 1 where scc_temp_id = ?"
        cur = conn.cursor()
        tup = (scc_temp_id,)
        res = cur.execute(q, tup)
        
    def load_exclusions(self, conn):
        if os.environ.get("DISABLE_SAT_EXCL", None) is None:
            qexcl = "select satrecord_id, siss_id, emplid from sat_exclude"
            cur = conn.cursor()
            for row in cur.execute(qexcl):
                satrecord_id, siss_id, emplid = row
                accum = f"{satrecord_id}_{siss_id}_{emplid}"
                if string_utils.hash_lu(self.exclusion_hash, accum) is None:
                    self.exclusion_hash[accum] = True
            cur.close()
        else:
            print("Disabling sat excl as DISABLE_SAT_EXCL is set")

    def is_exclusion(self, satrecord_id, siss_id, emplid):
        accum = f"{satrecord_id}_{siss_id}_{emplid}"
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
        satstage_update_arr = []
        hdr = sat_match_result.SatMatchResult.header()
        #pariahFilename = "sat_pariah1.txt"
        unmatchedFilename = "sat_unmatched1.txt"
        readyFilename = "sat_ready1.txt"
        exclFilename = "sat_excl.txt"
        #pariahFile = open(pariahFilename, "w", encoding="UTF-8")
        pariahFile = None
        unmatchedFile = open(unmatchedFilename, "w", encoding="UTF-8")
        readyFile = open(readyFilename, "w", encoding="UTF-8")
        exclFile = open(exclFilename, "w", encoding="UTF-8")
        
        #mydb_utils.uga_out(pariahFile,hdr)
        mydb_utils.uga_out(unmatchedFile,hdr)
        mydb_utils.uga_out(readyFile,hdr)
        mydb_utils.uga_out(exclFile,hdr)
    
        sat_count_q = """
        select count(*)
        from satstage s, satrecord r
        where s.satrecord_id = r.satrecord_id
        and s.query_pass = 0
        """
        cur = conn.cursor()
        row_count = 0
        for row in cur.execute(sat_count_q):
            row_count, = row
        print(f"the count is {row_count}")
        cur.close()
        
        # select the score data
        sat_data_q = """
        select distinct r.satrecord_id, s.scc_temp_id, r.last, r.first, r.mi,
        r.sex, r.dob, r.addr1, r.city, r.state, r.postal, r.country3, s.phone,
        s.email_addr, r2.hscode, r2.hsname, s.date_loaded, r2.cbsid
        from satstage s, satrecord r, satrecord_addl_2 r2
        where s.satrecord_id = r.satrecord_id
        and r.satrecord_id = r2.satrecord_id
        and s.query_pass = 0
        """
        cur = conn.cursor()
        i = 0
        for row in cur.execute(sat_data_q):
            satrecord_id, scc_temp_id, last, first, mi, sex, dob, addr1, city, state, postal, country, phone, email, hscode, hsname, loaddate, cbsid = row
            vdob = mydb_utils.iso_to_date(dob)
            #mydb_utils.uga_out(sys.stdout, ["sat_match_phone_haw",phone])

            #mydb_utils.uga_out(sys.stdout, [satrecord_id, scc_temp_id, last, first, mi, gender, vedob, street, city, state, zip5, hscode, email])
            
            # store the score data in a class
            satMatchSrc = sat_match_src.SatMatchSrc(satrecord_id, scc_temp_id, last, first, mi, sex, vdob, addr1, city, state, postal, country, phone, email, hscode, hsname, loaddate, cbsid)

            #print("commented out do_match1")
            # run match 1
            self.do_match1(conn, satMatchSrc, unmatchedFile, readyFile, pariahFile, satstage_update_arr)
            if (i % 100) == 0:
                pct = (100 * i / row_count)
                print(f"M1: i {i} pct {pct:.0f}")
            
            satMatchSrc = None
            i += 1
            
            
        #pariahFile.close()
        unmatchedFile.close()
        readyFile.close()
        exclFile.close()
        #print(f"pass1 satstage_update_arr count {len(satstage_update_arr)}")
        for i, scc_temp_id in enumerate(satstage_update_arr):
            self.update_query_pass(conn, scc_temp_id)
            if i % 10 == 0:
                print(i)
                conn.commit()
        conn.commit()
        
    #-------------------------------------------------
    def do_match1(self, conn, satMatchSrc, unmatchedFile, readyFile, pariahFile, update_arr):
        """do_match1() match applicant biodemo data using the score biodemo data as search
        parameters. Assume user has updated the test_consider field in applicant_biod"""
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

        # last <= last, first <= fi
        paras.append(string_utils.normalize(satMatchSrc.last.upper()))
        paras.append(satMatchSrc.first[0:1].upper())
        
        # last <= last, first <= mi
        paras.append(string_utils.normalize(satMatchSrc.last.upper()))
        paras.append(satMatchSrc.mi[0:1].upper())
        
        # first <= last,  last <= fi%   name reversal
        paras.append(string_utils.normalize(satMatchSrc.last.upper()))
        paras.append(satMatchSrc.first[0:1].upper() + '%')
        
        # first <= last,  last <= mi%   name reversal
        paras.append(string_utils.normalize(satMatchSrc.last.upper()))
        paras.append(satMatchSrc.mi[0:1].upper() + '%')
        
        # school ceeb code
        paras.append(satMatchSrc.hscode)
        #  and
        paras.append(satMatchSrc.dob)
        paras.append(satMatchSrc.postal.upper())
        paras.append(satMatchSrc.postal.upper())
        paras.append(satMatchSrc.city.upper())
        paras.append(satMatchSrc.city.upper())
        paras.append(satMatchSrc.city.upper())
        paras.append(satMatchSrc.hscode)
        paras.append(satMatchSrc.email.upper())
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
            satBiodemo = satMatchSrc.biodemo()
            homeMat = mat.compare1(satBiodemo, appHomeBiodemo)
            homeRat = homeMat.maRating
            mailMat = mat.compare1(satBiodemo, appMailBiodemo)
            mailRat = mailMat.maRating
            maxMat = match_result.MatchResult()
            maxRat = 0
            if homeRat >= mailRat:
                maxMat = homeMat
                maxRat = homeRat
            else:
                maxMat = mailMat
                maxRat = mailRat

            if not self.is_exclusion(satMatchSrc.satrecord_id, satMatchSrc.scc_temp_id, applicantBiod.emplid):
                if maxRat > 7:
                    if maxMat.isAutoMatch():
                        readyRes = sat_match_result.SatMatchResult(applicantBiod,satMatchSrc, maxMat)
                        mydb_utils.uga_out(readyFile, readyRes.toArr())
                    else:
                        unmatchedRes = sat_match_result.SatMatchResult(applicantBiod,satMatchSrc, maxMat)
                        mydb_utils.uga_out(unmatchedFile, unmatchedRes.toArr())
                    update_arr.append(satMatchSrc.scc_temp_id)
                else:
                    pass
                    #pariahRes = sat_match_result.SatMatchResult(applicantBiod,satMatchSrc, maxMat)
                    #mydb_utils.uga_out(pariahFile, pariahRes.toArr())
        pass # do_match1()

    #-------------------------------------------------
    def go2(self, conn):
        hdr = sat_match_result.SatMatchResult.header()

        #pariahFilename = "sat_pariah2.txt"
        unmatchedFilename = "sat_unmatched2.txt"
        readyFilename = "sat_ready2.txt"

        #pariahFile = open(pariahFilename, "w", encoding="UTF-8")
        pariahFile = None
        unmatchedFile = open(unmatchedFilename, "w", encoding="UTF-8")
        readyFile = open(readyFilename, "w", encoding="UTF-8")
        
        #mydb_utils.uga_out(pariahFile,hdr)
        mydb_utils.uga_out(unmatchedFile,hdr)
        mydb_utils.uga_out(readyFile,hdr)
    
        sat_count_q = """
        select count(*)
        from satstage s, satrecord r
        where s.satrecord_id = r.satrecord_id
        and s.query_pass = 0
        """
        cur = conn.cursor()
        row_count = 0
        for row in cur.execute(sat_count_q):
            row_count, = row
        print(f"the count is {row_count}")
        cur.close()
        
        sat_data_q = """
        select distinct r.satrecord_id, s.scc_temp_id, r.last, r.first, r.mi,
        r.sex, r.dob, r.addr1, r.city, r.state, r.postal, r.country3, r.phone, s.email_addr, r2.hscode, r2.hsname, s.date_loaded, r2.cbsid
        from satstage s, satrecord r, satrecord_addl_2 r2
        where s.satrecord_id = r.satrecord_id
        and r.satrecord_id = r2.satrecord_id
        and s.query_pass = 0
        """
        cur = conn.cursor()
        i = 0
        for row in cur.execute(sat_data_q):
            satrecord_id, scc_temp_id, last, first, mi, sex, dob, addr1, city, state, postal, country, phone, email, hscode, hsname, loaddate, cbsid = row
            vdob = mydb_utils.iso_to_date(dob)

            satMatchSrc = sat_match_src.SatMatchSrc(satrecord_id, scc_temp_id, last, first, mi, sex, vdob, addr1, city, state, postal, country, phone, email, hscode, hsname, loaddate, cbsid)

            self.do_match2(conn, satMatchSrc, unmatchedFile, readyFile, pariahFile)
            if (i % 100) == 0:
                pct = (100 * i / row_count)
                print(f"M2: i {i} pct {pct:.0f}")
            
            satMatchSrc = None
            i += 1
            
        #pariahFile.close()
        unmatchedFile.close()
        readyFile.close()

    #-----------------------------------------------
    def do_match2(self, conn, satMatchSrc, unmatchedFile, readyFile, pariahFile):
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
        paras.append(satMatchSrc.email.upper())
        paras.append(satMatchSrc.hscode)
        paras.append(satMatchSrc.dob)
        paras.append(satMatchSrc.postal.upper())
        paras.append(satMatchSrc.postal.upper())
        paras.append(string_utils.normalize(satMatchSrc.last.upper()))
        paras.append(satMatchSrc.city.upper())
        paras.append(satMatchSrc.city.upper())
        paras.append(satMatchSrc.city.upper())
        paras.append(satMatchSrc.first[0:1].upper())
        paras.append(satMatchSrc.mi[0:1].upper())
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
            satBiodemo = satMatchSrc.biodemo()
            homeMat = mat.compare1(satBiodemo, appHomeBiodemo)
            homeRat = homeMat.maRating
            mailMat = mat.compare1(satBiodemo, appMailBiodemo)
            mailRat = mailMat.maRating
            maxMat = match_result.MatchResult()
            maxRat = 0
            if homeRat >= mailRat:
                maxMat = homeMat
                maxRat = homeRat
            else:
                maxMat = mailMat
                maxRat = mailRat

            if not self.is_exclusion(satMatchSrc.satrecord_id, satMatchSrc.scc_temp_id, applicantBiod.emplid):
                if maxRat > 7:
                    if maxMat.isAutoMatch():
                        readyRes = sat_match_result.SatMatchResult(applicantBiod,satMatchSrc, maxMat)
                        mydb_utils.uga_out(readyFile, readyRes.toArr())
                    else:
                        unmatchedRes = sat_match_result.SatMatchResult(applicantBiod,satMatchSrc, maxMat)
                        mydb_utils.uga_out(unmatchedFile, unmatchedRes.toArr())
                else:
                    pass
                    #pariahRes = sat_match_result.SatMatchResult(applicantBiod,satMatchSrc, maxMat)
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
            print("use sat_match.py  1 | 2")
            sys.exit(-1)
            
        match_pass = sys.argv[1]
        if not match_pass in ["1","2"]:
            print("use sat_match.py  1 | 2")
            sys.exit(-1)

        b.load_exclusions(conn)
        b.go(conn, match_pass)
            
        fout = None
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
        
    finally:
        if not conn is None:
            conn.close()
        pass
main()

