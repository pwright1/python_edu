#!/usr/bin/env python3

# Copyright Sep 2025, Philip Wright. All rights reserved.

import sys
import hashlib
import traceback
import sqlite3
import os.path
import time
import re
#from datetime import datetime
from datetime import date
from pw_utils import toefl_match_result
from pw_utils import toefl_match_src
from pw_utils import toefl_utils
from pw_utils import applicant_biod
from pw_utils import match
from pw_utils import match_result
from pw_utils import match_util
from pw_utils import mydb_utils
from pw_utils import score_tables_utils
from pw_utils import string_utils


class Bork:
    debug = False
    def __init__(self):
        if self.debug:
            print("class {} init".format(type(self)) )
        self.exclusion_hash = {}

    def update_query_pass(self, conn, scc_temp_id):
        q = "update toefl_stage set query_pass = 1 where scc_temp_id = ?"
        cur = conn.cursor()
        tup = (scc_temp_id,)
        res = cur.execute(q, tup)
        
    def load_exclusions(self, conn):
        if os.environ.get("DISABLE_TOEFL_EXCL", None) is None:
            qexcl = "select toefl_record_id, scc_temp_id, emplid from toefl_exclude"
            cur = conn.cursor()
            for row in cur.execute(qexcl):
                toefl_record_id, scc_temp_id, emplid = row
                accum = f"{toefl_record_id}_{scc_temp_id}_{emplid}"
                if string_utils.hash_lu(self.exclusion_hash, accum) is None:
                    self.exclusion_hash[accum] = True
            cur.close()
        else:
            print("Disabling toefl excl as DISABLE_TOEFL_EXCL is set")

    def is_exclusion(self, toefl_record_id, scc_temp_id, emplid):
        accum = f"{toefl_record_id}_{scc_temp_id}_{emplid}"
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
        toefl_stage_update_arr = []
        hdr = toefl_match_result.ToeflMatchResult.header()
        #pariahFilename = "toefl_pariah1.txt"
        unmatchedFilename = "toefl_unmatched1.txt"
        readyFilename = "toefl_ready1.txt"
        exclFilename = "toefl_excl.txt"
        #pariahFile = open(pariahFilename, "w", encoding="UTF-8")
        pariahFile = None
        unmatchedFile = open(unmatchedFilename, "w", encoding="UTF-8")
        readyFile = open(readyFilename, "w", encoding="UTF-8")
        exclFile = open(exclFilename, "w", encoding="UTF-8")
        
        #mydb_utils.uga_out(pariahFile,hdr)
        mydb_utils.uga_out(unmatchedFile,hdr)
        mydb_utils.uga_out(readyFile,hdr)
        mydb_utils.uga_out(exclFile,hdr)
    
        toefl_count_q = """

        select count(*)
        from toefl_stage a, toefl_record b, toefl_file c
        where a.toefl_record_id = b.toefl_record_id
        and b.toefl_file_id = c.toefl_file_id
        and (a.emplid = '' or a.emplid is null)
        and a.query_pass = 0
        """
        cur = conn.cursor()
        row_count = 0
        for row in cur.execute(toefl_count_q):
            row_count, = row
        print(f"the count is {row_count}")
        cur.close()
        
        # select the score data
        toefl_data_q = """
        select b.toefl_record_id, a.scc_temp_id, b.last, b.first, b.middle, b.gender_ex, b.dob_ex, b.a1, b.a2, b.a3, b.a4, b.city, b.state, b.postal, b.country, b.email, b.admin_date, b.test_type, b.ibt_listening, b.ibt_reading, b.ibt_speaking, b.ibt_writing, b.ibt_total, b.pb_sec1, b.pb_sec2, b.pb_sec3, b.pb_conv_twe, b.pb_total, b.rpdt_listening, b.rpdt_reading, b.rpdt_writing, b.ran, c.filename, b.fileline
        from toefl_stage a, toefl_record b, toefl_file c
        where a.toefl_record_id = b.toefl_record_id
        and b.toefl_file_id = c.toefl_file_id
        and (a.emplid = '' or a.emplid is null)
        and a.query_pass = 0
        order by b.last, b.first, b.middle
        """
        cur = conn.cursor()
        i = 0
        for row in cur.execute(toefl_data_q):
            toefl_record_id, scc_temp_id, last, first, middle, sex, dob, a1,a2,a3,a4, city, state, postal, country, email, admin_date, test_type, ibt_listening, ibt_reading, ibt_speaking, ibt_writing, ibt_total, pb_sec1, pb_sec2, pb_sec3, pb_conv_twe, pb_total, rpdt_listening, rpdt_reading, rpdt_writing, ran, filename, fileline = row
            vdob = mydb_utils.iso_to_date(dob)
            
            # store the score data in a class
            toeflMatchSrc = toefl_match_src.ToeflMatchSrc(toefl_record_id, scc_temp_id, last, first, middle, sex, vdob, a1, a2, a3, a4, city, state, postal, country, email)

            toeflScore = toefl_match_result.ToeflScore(admin_date, test_type, ibt_listening, ibt_reading, ibt_speaking, ibt_writing, ibt_total, pb_sec1, pb_sec2, pb_sec3, pb_conv_twe, pb_total, rpdt_listening, rpdt_reading, rpdt_writing, ran, filename, fileline)
            
            #print("commented out do_match1")
            # run match 1
            self.do_match1(conn, toeflMatchSrc, unmatchedFile, readyFile, pariahFile, toefl_stage_update_arr, toeflScore)
            if (i % 100) == 0:
                pct = (100 * i / row_count)
                print(f"M1: i {i} pct {pct:.0f}")
            
            toeflMatchSrc = None
            i += 1
            
            
        #pariahFile.close()
        unmatchedFile.close()
        readyFile.close()
        exclFile.close()
        #print(f"pass1 toefl_stage_update_arr count {len(toefl_stage_update_arr)}")
        for i, scc_temp_id in enumerate(toefl_stage_update_arr):
            self.update_query_pass(conn, scc_temp_id)
            if i % 10 == 0:
                print(i)
                conn.commit()
        conn.commit()
        
    #-------------------------------------------------
    def do_match1(self, conn, toeflMatchSrc, unmatchedFile, readyFile, pariahFile, update_arr, toeflScore):
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
        (upper(first) = ? and upper(last) like ?)
        )
        and
        (
        (birthdate = ?)  or
        (upper(substr(mpostal,1,5)) = ? and mpostal <> '') or
        (upper(substr(hpostal,1,5)) = ? and hpostal <> '') or
        (upper(mcity) = ? and mcity <> '') or
        (upper(hcity) = ? and hcity <> '') or
        (upper(scity) = ? and scity <> '') or
        (upper(email) = ? and email <> '')
        )
        """
        paras = []

        # last <= last, first <= fi
        paras.append(string_utils.normalize(toeflMatchSrc.last.upper()))
        paras.append(toeflMatchSrc.first[0:1].upper())
        
        # last <= last, first <= mi
        paras.append(string_utils.normalize(toeflMatchSrc.last.upper()))
        paras.append(toeflMatchSrc.middle[0:1].upper())
        
        # first <= last,  last <= fi%   name reversal
        paras.append(string_utils.normalize(toeflMatchSrc.last.upper()))
        paras.append(toeflMatchSrc.first[0:1].upper() + '%')
        
        # first <= last,  last <= mi%   name reversal
        paras.append(string_utils.normalize(toeflMatchSrc.last.upper()))
        paras.append(toeflMatchSrc.middle[0:1].upper() + '%')
        
        #  and
        paras.append(toeflMatchSrc.dob)
        paras.append(toeflMatchSrc.postal.upper())
        paras.append(toeflMatchSrc.postal.upper())
        paras.append(toeflMatchSrc.city.upper())
        paras.append(toeflMatchSrc.city.upper())
        paras.append(toeflMatchSrc.city.upper())
        paras.append(toeflMatchSrc.email.upper())
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
            toeflBiodemo = toeflMatchSrc.biodemo()
            homeMat = mat.compare1(toeflBiodemo, appHomeBiodemo)
            homeRat = homeMat.maRating
            mailMat = mat.compare1(toeflBiodemo, appMailBiodemo)
            mailRat = mailMat.maRating
            maxMat = match_result.MatchResult()
            maxRat = 0
            if homeRat >= mailRat:
                maxMat = homeMat
                maxRat = homeRat
            else:
                maxMat = mailMat
                maxRat = mailRat

            if not self.is_exclusion(toeflMatchSrc.toefl_record_id, toeflMatchSrc.scc_temp_id, applicantBiod.emplid):
                if maxRat > 7:
                    if maxMat.isAutoMatch():
                        readyRes = toefl_match_result.ToeflMatchResult(applicantBiod,toeflMatchSrc, maxMat, toeflScore)
                        mydb_utils.uga_out(readyFile, readyRes.toArr())
                    else:
                        unmatchedRes = toefl_match_result.ToeflMatchResult(applicantBiod,toeflMatchSrc, maxMat,toeflScore)
                        mydb_utils.uga_out(unmatchedFile, unmatchedRes.toArr())
                    update_arr.append(toeflMatchSrc.scc_temp_id)
                else:
                    pass
                    #pariahRes = toefl_match_result.ToeflMatchResult(applicantBiod,toeflMatchSrc, maxMat)
                    #mydb_utils.uga_out(pariahFile, pariahRes.toArr())
        pass # do_match1()

    #-------------------------------------------------
    def go2(self, conn):

        toefl_stage_update_arr = []
        hdr = toefl_match_result.ToeflMatchResult.header()
        #pariahFilename = "toefl_pariah1.txt"
        unmatchedFilename = "toefl_unmatched2.txt"
        readyFilename = "toefl_ready2.txt"
        exclFilename = "toefl_excl.txt"
        #pariahFile = open(pariahFilename, "w", encoding="UTF-8")
        pariahFile = None
        unmatchedFile = open(unmatchedFilename, "w", encoding="UTF-8")
        readyFile = open(readyFilename, "w", encoding="UTF-8")
        exclFile = open(exclFilename, "w", encoding="UTF-8")
        
        #mydb_utils.uga_out(pariahFile,hdr)
        mydb_utils.uga_out(unmatchedFile,hdr)
        mydb_utils.uga_out(readyFile,hdr)
        mydb_utils.uga_out(exclFile,hdr)
    
        toefl_count_q = """
        select count(*)
        from toefl_stage a, toefl_record b, toefl_file c
        where a.toefl_record_id = b.toefl_record_id
        and b.toefl_file_id = c.toefl_file_id
        and (a.emplid = '' or a.emplid is null)
        and a.query_pass = 0
        """
        cur = conn.cursor()
        row_count = 0
        for row in cur.execute(toefl_count_q):
            row_count, = row
        print(f"the count is {row_count}")
        cur.close()
        
        # select the score data
        toefl_data_q = """
        select b.toefl_record_id, a.scc_temp_id, b.last, b.first, b.middle, b.gender_ex, b.dob_ex, b.a1, b.a2, b.a3, b.a4, b.city, b.state, b.postal, b.country, b.email, b.admin_date, b.test_type, b.ibt_listening, b.ibt_reading, b.ibt_speaking, b.ibt_writing, b.ibt_total, b.pb_sec1, b.pb_sec2, b.pb_sec3, b.pb_conv_twe, b.pb_total, b.rpdt_listening, b.rpdt_reading, b.rpdt_writing, b.ran, c.filename, b.fileline
        from toefl_stage a, toefl_record b, toefl_file c
        where a.toefl_record_id = b.toefl_record_id
        and b.toefl_file_id = c.toefl_file_id
        and (a.emplid = '' or a.emplid is null)
        and a.query_pass = 0
        order by b.last, b.first, b.middle
        """
        cur = conn.cursor()
        i = 0
        for row in cur.execute(toefl_data_q):
            toefl_record_id, scc_temp_id, last, first, middle, sex, dob, a1,a2,a3,a4, city, state, postal, country, email, admin_date, test_type, ibt_listening, ibt_reading, ibt_speaking, ibt_writing, ibt_total, pb_sec1, pb_sec2, pb_sec3, pb_conv_twe, pb_total, rpdt_listening, rpdt_reading, rpdt_writing, ran, filename, fileline = row
            vdob = mydb_utils.iso_to_date(dob)
            
            # store the score data in a class
            toeflMatchSrc = toefl_match_src.ToeflMatchSrc(toefl_record_id, scc_temp_id, last, first, middle, sex, vdob, a1, a2, a3, a4, city, state, postal, country, email)

            toeflScore = toefl_match_result.ToeflScore(admin_date, test_type, ibt_listening, ibt_reading, ibt_speaking, ibt_writing, ibt_total, pb_sec1, pb_sec2, pb_sec3, pb_conv_twe, pb_total, rpdt_listening, rpdt_reading, rpdt_writing, ran, filename, fileline)
            
            #print("commented out do_match1")
            # run match 1
            self.do_match2(conn, toeflMatchSrc, unmatchedFile, readyFile, pariahFile, toefl_stage_update_arr, toeflScore)
            if (i % 100) == 0:
                pct = (100 * i / row_count)
                print(f"M2: i {i} pct {pct:.0f}")
            
            toeflMatchSrc = None
            i += 1
            
            
        #pariahFile.close()
        unmatchedFile.close()
        readyFile.close()
        exclFile.close()
        #print(f"pass1 toefl_stage_update_arr count {len(toefl_stage_update_arr)}")
        for i, scc_temp_id in enumerate(toefl_stage_update_arr):
            self.update_query_pass(conn, scc_temp_id)
            if i % 10 == 0:
                print(i)
                conn.commit()
        conn.commit()


    #-----------------------------------------------
    def do_match2(self, conn, toeflMatchSrc, unmatchedFile, readyFile, pariahFile, update_arr, toeflScore):
        mat = match.Match()
        mu = match_util.MatchUtil()
        qrest = """
        select distinct slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, cellphone, homephone, fatherphone, motherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, mpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, hpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, spostal, scountry, orgid, ceeb, test_consider from applicant_biod
        where test_consider in ('', '1')
        and
        (upper(email) = ? and email <> '') or
        (
        (
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
        paras.append(toeflMatchSrc.email.upper())
        paras.append(toeflMatchSrc.dob)
        paras.append(toeflMatchSrc.postal.upper())
        paras.append(toeflMatchSrc.postal.upper())
        paras.append(string_utils.normalize(toeflMatchSrc.last.upper()))
        paras.append(toeflMatchSrc.city.upper())
        paras.append(toeflMatchSrc.city.upper())
        paras.append(toeflMatchSrc.city.upper())
        paras.append(toeflMatchSrc.first[0:1].upper())
        paras.append(toeflMatchSrc.middle[0:1].upper())
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
            toeflBiodemo = toeflMatchSrc.biodemo()
            homeMat = mat.compare1(toeflBiodemo, appHomeBiodemo)
            homeRat = homeMat.maRating
            mailMat = mat.compare1(toeflBiodemo, appMailBiodemo)
            mailRat = mailMat.maRating
            maxMat = match_result.MatchResult()
            maxRat = 0
            if homeRat >= mailRat:
                maxMat = homeMat
                maxRat = homeRat
            else:
                maxMat = mailMat
                maxRat = mailRat

            if not self.is_exclusion(toeflMatchSrc.toefl_record_id, toeflMatchSrc.scc_temp_id, applicantBiod.emplid):
                if maxRat > 7:
                    if maxMat.isAutoMatch():
                        readyRes = toefl_match_result.ToeflMatchResult(applicantBiod,toeflMatchSrc, maxMat, toeflScore)
                        mydb_utils.uga_out(readyFile, readyRes.toArr())
                    else:
                        unmatchedRes = toefl_match_result.ToeflMatchResult(applicantBiod,toeflMatchSrc, maxMat,toeflScore)
                        mydb_utils.uga_out(unmatchedFile, unmatchedRes.toArr())
                    update_arr.append(toeflMatchSrc.scc_temp_id)
                else:
                    pass
                    #pariahRes = toefl_match_result.ToeflMatchResult(applicantBiod,toeflMatchSrc, maxMat)
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
            print("use toefl_match.py  1 | 2")
            sys.exit(-1)
            
        match_pass = sys.argv[1]
        if not match_pass in ["1","2"]:
            print("use toefl_match.py  1 | 2")
            sys.exit(-1)

        b.load_exclusions(conn)
        b.go(conn, match_pass)
            
        fout = None
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
        sys.exit(-1)
        
    finally:
        if not conn is None:
            conn.close()
        pass
main()

