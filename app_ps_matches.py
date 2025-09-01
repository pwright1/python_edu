#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import time
#import csv
from pw_utils import mydb_utils
from pw_utils import applicant_biod
from pw_utils import apool_last
from pw_utils import match
from pw_utils import match_result
from pw_utils import match_util
from pw_utils import string_utils
from pw_utils import app_ps_main_susp
from pw_utils import app_match_result
import re
from operator import attrgetter # for sorted attrgetter


note = """
"""

class Bork:
    debug = False
    
    #--------------------------------------
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        pass

    def do_match(self, conn, function):

        file_prefix = ""
        # current year apps
        if function == "ps":
            file_prefix = "apool"
        # previous 5 years apps
        elif function == "pslast":
            file_prefix = "alast"
        # precollege
        elif function == "prec":
            file_prefix = "prec"
        else:
            raise RuntimeError(f"unknown match function {function}")
        
        ready_arr = []
        unmatched_arr = []
        
        q_count = """
        select count(*)
        from app_ps_main
        where appno = ''
        and loaddate = ''
        and ps_update_date = ''
        """

        # need to add ssn here as pslast and prec need it
        qsusp = """
        select id, slate_person_id, name_last, name_first, name_middle, gender, birth_date, address_address1_mail, address_city_mail, address_address_state_mail, address_address_postal_mail, address_country_mail,email_address_home,phone_number_cell, last_school_attended, national_id
        from app_ps_main
        where appno = ''
        and loaddate = ''
        and ps_update_date = ''
        order by name_last, name_first, name_middle
        """
        cur = conn.cursor()
        row_count = 0
        for row in cur.execute(q_count):
            row_count, = row
        cur.close()
        print(f"row count {row_count}")
        pct_modulus = row_count // 100
        if pct_modulus < 10:
            pct_modulus = 10
        
        cur = conn.cursor()
        i = 0
        for row in cur.execute(qsusp):
            id, slate_person_id, name_last, name_first, name_middle, gender, birth_date, address_address1_mail, address_city_mail, address_state_mail, address_postal_mail, address_country_mail,email_address_home,phone_number_cell, orgid, nid = row

            vpostal = address_postal_mail
            if len(vpostal) > 5:
                vpostal = vpostal[0:5]

            vdob = mydb_utils.psdate_to_iso(birth_date)

            vmiddle = name_middle    
            if len(vmiddle) > 0:
                vmiddle = vmiddle[0:1].upper()

            # store data into a class
            # call the match function

            app_susp  = app_ps_main_susp.AppPsMainSusp(id, slate_person_id, name_last, name_first,vmiddle, gender, vdob, address_address1_mail, address_city_mail, address_state_mail, vpostal, address_country_mail,email_address_home,phone_number_cell, orgid, nid)

            if function == "ps":
                self.do_match_ps(conn, app_susp, ready_arr, unmatched_arr)

            elif function == "pslast":
                self.do_match_pslast(conn, app_susp, ready_arr, unmatched_arr)

            elif function == "prec":
                self.do_match_prec(conn, app_susp, ready_arr, unmatched_arr)
            
            if (i % pct_modulus) == 0:
                pct = (100 * i / row_count)
                print(f"M: i {i:5d} pct {pct:>3.0f}")
                
            i += 1

        print(f"ready {len(ready_arr)} unmatched {len(unmatched_arr)}")
        vready_arr     = sorted(ready_arr,     key=attrgetter('dob','fn','ape','rat'), reverse=True)
        vunmatched_arr = sorted(unmatched_arr, key=attrgetter('dob','fn','ape','rat'), reverse=True)

        unmatchedFilename = f"{function}_unmatched.csv"
        readyFilename = f"{function}_ready.csv"

        #unmatchedFile = open(unmatchedFilename, "w", encoding="UTF-8")
        #readyFile = open(readyFilename, "w", encoding="UTF-8")

        hdr = app_match_result.AppMatchResult.header()
        with open(unmatchedFilename, "w") as unmatchedFile:
            mydb_utils.uga_out(unmatchedFile,hdr)
            for entry in vunmatched_arr:
                mydb_utils.uga_out(unmatchedFile, entry.toArr())
        
        with open(readyFilename, "w") as readyFile:
            mydb_utils.uga_out(readyFile,hdr)
            for entry in vready_arr:
                mydb_utils.uga_out(readyFile, entry.toArr())
        time.sleep(2)
        
        mydb_utils.csv_to_xlsx(readyFilename)
        mydb_utils.csv_to_xlsx(unmatchedFilename)

        
                
    #-------------------------------------
    def do_match_ps(self, conn, asusp, ready_arr, unmatched_arr):
        
        mat = match.Match()
        mu = match_util.MatchUtil()
        q = """
        select slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, cellphone, homephone, fatherphone, motherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, mpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, hpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, spostal, scountry, orgid, ceeb, test_consider from scores.applicant_biod
        where 1 = 1
        and
        (
        (upper(last) = ?  and upper(substr(first,1,1)) = ?) or
        (upper(last) = ?  and upper(substr(first,1,1)) = ?) or
        (upper(first) = ? and upper(last) like ?) or   
        (upper(first) = ? and upper(last) like ?) or
        (orgid = ? and orgid <> '' )
        )
        and
        (
        (birthdate = ?)  or
        (upper(substr(mpostal,1,5)) = ? and mpostal <> '') or
        (upper(substr(hpostal,1,5)) = ? and hpostal <> '') or
        (upper(mcity) = ? and mcity <> '') or
        (upper(hcity) = ? and hcity <> '') or
        (upper(scity) = ? and scity <> '') or
        (orgid = ? and orgid <> '') or
        (upper(email) = ? and email <> '')
        )
        """
        paras = []
        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_first[0:1].upper())
        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_middle[0:1].upper())
        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_first[0:1].upper() + '%')
        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_middle[0:1].upper() + '%')
        paras.append(asusp.orgid)
        paras.append(asusp.birth_date)
        paras.append(asusp.address_address_postal_mail[0:5])
        paras.append(asusp.address_address_postal_mail[0:5])
        paras.append(asusp.address_city_mail.upper())
        paras.append(asusp.address_city_mail.upper())
        paras.append(asusp.address_city_mail.upper())
        paras.append(asusp.orgid)
        paras.append(asusp.email_address_home.upper())
        tparas = tuple(paras)
        cur = conn.cursor()
        rownum = 0
        for row in cur.execute(q, tparas):
            slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, cellphone, homephone, fatherphone, motherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, mpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, hpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, spostal, scountry, orgid, ceeb, test_consider = row            

            vhomephone = mu.phone_cleanup(homephone)
            vcellphone = mu.phone_cleanup(cellphone)
            vmotherphone = mu.phone_cleanup(motherphone)
            vfatherphone = mu.phone_cleanup(fatherphone)

            vmpostal = re.sub(r"\-","",mpostal[0:5])
            vhpostal = re.sub(r"\-","",hpostal[0:5])
            vspostal = re.sub(r"\-","",spostal[0:5])
            
            vdob = mydb_utils.date_to_iso(birthdate)

            # putting orgid in ceeb field as biodeom has no orgid field
            vceeb = orgid
            
            applicantBiod = applicant_biod.ApplicantBiod(slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, vdob, email, fatheremail, motheremail, vcellphone, vhomephone, vfatherphone, vmotherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, vmpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, vhpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, vspostal, scountry, orgid, vceeb, test_consider)

            #print(rownum)
            
            appHomeBiodemo = applicantBiod.homeBiodemo()
            appMailBiodemo = applicantBiod.mailBiodemo()
            asuspBiodemo = asusp.biodemo()
            homeMat = mat.compare1(asuspBiodemo, appHomeBiodemo)
            homeRat = homeMat.maRating
            mailMat = mat.compare1(asuspBiodemo, appMailBiodemo)
            mailRat = mailMat.maRating
            maxMat = match_result.MatchResult()
            maxRat = 0
            if homeRat >= mailRat:
                maxMat = homeMat
                maxRat = homeRat
            else:
                maxMat = mailMat
                maxRat = mailRat

            if maxRat > 8:
                print(f"max Rat {maxRat}")
                if maxMat.isAutoMatch():
                    readyRes = app_match_result.AppMatchResult(applicantBiod, asusp, maxMat, blank_ssn=True)
                    ready_arr.append(readyRes)
                else:
                    unmatchedRes = app_match_result.AppMatchResult(applicantBiod, asusp, maxMat, blank_ssn=True)
                    unmatched_arr.append(unmatchedRes)
            else:
                pass
            
            rownum += 1
                
        pass

    #-------------------------------------
    def do_match_pslast(self, conn, asusp, ready_arr, unmatched_arr):
        
        mat = match.Match()
        mu = match_util.MatchUtil()
        print(f"NOTE YEAR 2025 EXCLUSION NEEDS REMOVAL HERE do_match_pslast()")
        q = """
        select distinct id,emplid,appno,last,first,middle,pref,suffix,dob,nid,a1,a2,city,state,postal,country,phone,email, sex, org
      from apool_last
      where
      (
        (upper(last) = ?  and upper(substr(first,1,1)) = ?) or
        (upper(last) = ?  and upper(substr(first,1,1)) = ?) or
        (upper(first) = ? and upper(last) like ?) or   
        (upper(first) = ? and upper(last) like ?)
      )
      and
      (
        (dob = ?)  or
        (upper(substr(postal,1,5)) = ? and postal <> '') or
        (upper(city) = ? and city <> '') or
        (upper(email) = ? and email <> '')
      )
        and year <> '2025'
        """
        paras = []
        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_first[0:1].upper())
        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_middle[0:1].upper())

        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_first[0:1].upper() + '%')
        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_middle[0:1].upper()+ '%')

        paras.append(asusp.birth_date)
        paras.append(asusp.address_address_postal_mail[0:5])
        paras.append(asusp.address_city_mail.upper())
        paras.append(asusp.email_address_home.upper())
        tparas = tuple(paras)
        #print(tparas)
        cur = conn.cursor()
        rownum = 0
        for row in cur.execute(q, tparas):
            id,emplid,appno,last,first,middle,pref,suffix,dob,nid,a1,a2,city,state,postal,country,phone,email,sex, org = row

            vphone = mu.phone_cleanup(phone)
            vpostal = re.sub(r"\-","",postal[0:5])
            vdob = mydb_utils.date_to_iso(dob)
            
            apoolLast = apool_last.ApoolLast(id,emplid,appno,last,first,middle,pref,suffix,vdob,nid,a1,a2,city,state,vpostal,country,vphone,email,sex, org)
            
            appHomeBiodemo = apoolLast.homeBiodemo()
            appMailBiodemo = apoolLast.mailBiodemo()
            asuspBiodemo = asusp.biodemo()
            homeMat = mat.compare1(asuspBiodemo, appHomeBiodemo)
            homeRat = homeMat.maRating
            mailMat = mat.compare1(asuspBiodemo, appMailBiodemo)
            mailRat = mailMat.maRating
            maxMat = match_result.MatchResult()
            maxRat = 0
            if homeRat >= mailRat:
                maxMat = homeMat
                maxRat = homeRat
            else:
                maxMat = mailMat
                maxRat = mailRat
 
            if maxRat > 8:
                print(f"max Rat {maxRat}")
                if maxMat.isAutoMatch():
                    readyRes = app_match_result.AppMatchResult(apoolLast, asusp, maxMat, blank_ssn=False)
                    ready_arr.append(readyRes)
                else:
                    unmatchedRes = app_match_result.AppMatchResult(apoolLast, asusp, maxMat, blank_ssn=False)
                    unmatched_arr.append(unmatchedRes)
            else:
                pass
            rownum += 1
        pass

    #-------------------------------------
    def do_match_prec(self, conn, asusp, ready_arr, unmatched_arr):
        mat = match.Match()
        mu = match_util.MatchUtil()
        q = """
        select distinct id,emplid,last,first,middle,pref,suffix,dob,nid,a1,a2,city,state,postal,country,phone,email, sex, ''
      from prec_pool
      where
      (
        (upper(last) = ?  and upper(substr(first,1,1)) = ?) or
        (upper(last) = ?  and upper(substr(first,1,1)) = ?) or
        (upper(first) = ? and upper(last) like ?) or   
        (upper(first) = ? and upper(last) like ?)
      )
      and
      (
        (dob = ?)  or
        (upper(substr(postal,1,5)) = ? and postal <> '') or
        (upper(city) = ? and city <> '') or
        (upper(email) = ? and email <> '')
      )
        """
        paras = []
        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_first[0:1].upper())
        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_middle[0:1].upper())

        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_first[0:1].upper() + '%')
        paras.append(string_utils.normalize(asusp.name_last.upper()))
        paras.append(asusp.name_middle[0:1].upper()+ '%')

        paras.append(asusp.birth_date)
        paras.append(asusp.address_address_postal_mail[0:5])
        paras.append(asusp.address_city_mail.upper())
        paras.append(asusp.email_address_home.upper())
        tparas = tuple(paras)
        #print(tparas)
        cur = conn.cursor()
        rownum = 0
        for row in cur.execute(q, tparas):
            id,emplid,last,first,middle,pref,suffix,dob,nid,a1,a2,city,state,postal,country,phone,email,sex, org = row

            vphone = mu.phone_cleanup(phone)
            vpostal = re.sub(r"\-","",postal[0:5])
            vdob = mydb_utils.date_to_iso(dob)
            vappno = ''
            
            apoolLast = apool_last.ApoolLast(id,emplid,vappno,last,first,middle,pref,suffix,vdob,nid,a1,a2,city,state,vpostal,country,vphone,email,sex, org)
            
            appHomeBiodemo = apoolLast.homeBiodemo()
            appMailBiodemo = apoolLast.mailBiodemo()
            asuspBiodemo = asusp.biodemo()
            homeMat = mat.compare1(asuspBiodemo, appHomeBiodemo)
            homeRat = homeMat.maRating
            mailMat = mat.compare1(asuspBiodemo, appMailBiodemo)
            mailRat = mailMat.maRating
            maxMat = match_result.MatchResult()
            maxRat = 0
            if homeRat >= mailRat:
                maxMat = homeMat
                maxRat = homeRat
            else:
                maxMat = mailMat
                maxRat = mailRat
 
            if maxRat > 8:
                print(f"max Rat {maxRat}")
                if maxMat.isAutoMatch():
                    readyRes = app_match_result.AppMatchResult(apoolLast, asusp, maxMat, blank_ssn=False)
                    ready_arr.append(readyRes)
                else:
                    unmatchedRes = app_match_result.AppMatchResult(apoolLast, asusp, maxMat, blank_ssn=False)
                    unmatched_arr.append(unmatchedRes)
            else:
                pass
            rownum += 1
        pass
        
            
    #--------------------------------------
    def go(self, conn):
        if len(sys.argv[1:]) != 0:
            print("use app_ps_matches.py")
            sys.exit(-1)

        print("doing ps match")
        self.do_match(conn, function="ps")

        print("doing pslast match")
        self.do_match(conn, function="pslast")
        
        print("doing precollege match")
        self.do_match(conn, function="prec")

        # to do the missing HS, you need to add to the PS queries HS data fields, and load them
        # with the appload. all records with the org field blank
        
        pass # def go
    pass # class1
        
def main():
    conn = None
    try:
        apps_db_name = "apps.db"
        pools_db_name = "pools.db"
        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()

        apps_db_file = os.path.join(db_dir, apps_db_name)
        if not os.path.exists(apps_db_file):
            raise RuntimeError(f"apps db file {apps_db_file} not found")

        pools_db_file = os.path.join(db_dir, pools_db_name)
        if not os.path.exists(pools_db_file):
            raise RuntimeError(f"pools db file {pools_db_file} not found")

        scores_db_file = os.path.join(db_dir, scores_db_name)
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")

        conn = mydb_utils.sqlite3_connect(apps_db_file)
        cur = conn.cursor()
        cur.execute(f"attach database '{pools_db_file}' as pools")
        cur.execute(f"attach database '{scores_db_file}' as scores")
        cur.close
        
        b = Bork()
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()
