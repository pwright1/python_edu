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
from pw_utils import mydb_utils
from pw_utils import score_tables_utils
from pw_utils import act_utils
from pw_utils import sat_utils

"""
"""

class Bork:
    debug = False
    out_hdr = ["SCC_TEMP_ID","TEST_ID","LS_DATA_SOURCE","DATE_LOADED","LAST_NAME","FIRST_NAME","MIDDLE_NAME","EMAIL_ADDR","ADDRESS1","CITY","STATE","POSTAL","COUNTRY","BIRTHDATE","HPHONE","STATUS","CREATED","LASTUPD","EMPLID"]

    score_types = [
        'ERWS',
        'MSS',
        'TOTAL',
    ]

    max_test_age_yrs = 6
    
    def delete(self, conn):
        delete_q = "delete from satstage"
     
    def __init__(self):
        #tstamp = mydb_utils.get_file_ts()
        #print(tstamp)
        if self.debug:
            print("class {} init".format(type(self)) )
        self.score_type_dict = {}
    
    def dump_score_types(self):
        # for enumerating all the test score types used in development of this script
        print(list(self.score_type_dict))
            

    def process_hash(self, dict, line_count, fout, conn, skip_insert):
        # dict contains one record of data

        key_list = list(dict)
        scc_temp_id = key_list[0]
        field_dict = dict[scc_temp_id]
        field_keys = list(field_dict)
        
        column_arr = []
        for field in self.out_hdr:
            value = field_dict[field]
            column_arr.append(str(value))
        
        # need the score data also
        # an array of hash objects
        scores_hash = {}
        scores_column_arr = []
        scores_arr = field_dict['scores']
        for score_dict in scores_arr:
            test_comp = score_dict['test_component']
            test_score = score_dict['score']
            test_dt = score_dict['test_dt']
            vtest_dt = act_utils.aydate_to_act_dt(test_dt)
            self.score_type_dict[test_comp] = "y"
            scores_hash[test_comp] = (test_score,vtest_dt)
        
        for score_type in self.score_types:
            scores_column_arr.append(scores_hash.get(score_type,("","")))
     
        # writing to a file, Need to put into database here
        if not fout is None:
            mydb_utils.uga_out(fout, column_arr + scores_column_arr)
        if not skip_insert:
            digarr = []

            scc_temp_id, test_id, ls_data_source, date_loaded, last_name, first_name, middle_name, email_addr, address1, city, state, postal, country, birthdate, hphone, status, created, lastupd, emplid = column_arr

            erws, mss, total = scores_column_arr

            erws_sc, erws_dt = erws
            mss_sc, mss_dt = mss
            total_sc, total_dt = total

            vbirthdate = mydb_utils.date_to_iso(birthdate)
            vdate_loaded = mydb_utils.date_to_iso(date_loaded)
            
            
            arr = [scc_temp_id, test_id, ls_data_source, vdate_loaded, last_name, first_name, middle_name, email_addr, address1, city, state, postal, country, vbirthdate, hphone, status, created, lastupd, emplid, erws_sc, erws_dt, mss_sc, mss_dt, total_sc, total_dt]
            
            vlast = last_name.upper()[0:30]
            vfirst = first_name.upper()[0:30]
            vmi = middle_name.upper()[0:1]
            vdob = ""
            if len(birthdate) == 10:
                vdob = birthdate[6:10] + '-' + birthdate[0:2] + '-' +  birthdate[3:5]
            vemail = email_addr.upper()[0:50]
            vstreet = address1.upper()[0:40]
            vcity = city.upper()[0:30]
            vpostal = ""
            vstate = ""
            if country == "USA":
                vstate = state.upper()[0:2]
                vpostal = postal.upper()[0:5]
            
            digest_test_dt = f"{test_dt[6:10]}{test_dt[0:2]}"
            digparts_enc, hexdigest = sat_utils.calc_digest(vdate_loaded,
                                                            self.float_str_to_02_dec(erws_sc),
                                                            erws_dt,
                                                            self.float_str_to_02_dec(mss_sc),
                                                            mss_dt,
                                                            self.float_str_to_02_dec(total_sc),
                                                            total_dt,
                                                            vlast,
                                                            vfirst,
                                                            vmi,
                                                            vdob,
                                                            vemail,
                                                            vstreet,
                                                            vcity,
                                                            vstate,
                                                            vpostal)
            hexdigest_nld = sat_utils.calc_digest_nld(self.float_str_to_02_dec(erws_sc),
                                                  erws_dt,
                                                  self.float_str_to_02_dec(mss_sc),
                                                  mss_dt,
                                                  self.float_str_to_02_dec(total_sc),
                                                  total_dt,
                                                  vlast,
                                                  vfirst,
                                                  vmi,
                                                  vdob,
                                                  vemail,
                                                  vstreet,
                                                  vcity,
                                                  vstate,
                                                  vpostal)
            
            arr.append(hexdigest)
            arr.append(digparts_enc)
            arr.append(hexdigest_nld)
            #print(f"before insert cols {len(arr)}")
            self.insert(conn, arr)
        if self.debug:
            print("process hash line {}".format(line_count))
            dict_list = list(dict)
            for key in dict_list:
                print("key {}".format(key))
                print(dict[key])
        dict.clear()
        pass #def ()

    def float_str_to_02_dec(self, sval):
        if sval == '' or sval == '--':
            return ''
        ival = int(float(sval))
        sival = f"{ival:02d}"
        return sival
        
    def read_ps_sat_stage_query_file(self, fout, conn):
        
        dict = {}
        now = date.today()
        expected_fields = 22
        if len(sys.argv[1:]) == 0:
            print("use _ filename.csv")
            sys.exit()
        fname = sys.argv[1]

        if len(fname) < 27:
            raise RuntimeError ("File name needs to have _1505_SAT in it to load")

        match_arr = re.findall(r"_1505_SAT",fname)
        if len(match_arr) != 1 or match_arr[0] != "_1505_SAT":
            raise RuntimeError ("File name needs to have _1505_SAT in it to load")
        
        if self.debug:
            print("the filename is {}".format(fname))
        with open(fname, "r", encoding="UTF-8") as file:
            line_count = 0
            last_scc_temp_id = ""
            done = False
            while not done:
                line = file.readline()
                if len(line) == 0:
                    done = True
                    continue
                # skip header line
                if line_count == 0:
                    line_count += 1
                    continue
                skip_insert = False
                if self.debug:
                    print("while loop")
                vline = line.strip()
                fields = mydb_utils.split_sq_csv_line(vline)
                nfields = len(fields)
                if nfields != expected_fields:
                    print("field count {} not expected {}".format(nfields, expected_fields))
                    sys.exit(1)
                (scc_temp_id, test_id, test_component, test_dt, ls_data_source, score, date_loaded, last_name, first_name, middle_name, email_addr, address1, city, state, postal, country, birthdate, hphone, status, created, lastupd, emplid) = fields

                #ignore SAT2 scores. # we don't require and no longer offered. 
                if test_id != "SAT1":
                    line_count += 1
                    continue
                
                test_dt_mon, test_dt_day, test_dt_yr = test_dt.split("/")
                test_date = date.fromisoformat(f"{test_dt_yr}-{test_dt_mon}-{test_dt_day}")
                test_age_years = int((now - test_date).days / 365)
                #print(f"age {test_age_years}")
                if test_age_years > self.max_test_age_yrs:
                    skip_insert = True
                    
                if(scc_temp_id != last_scc_temp_id and line_count > 1):
                    self.process_hash(dict, line_count, fout, conn, skip_insert)
                
                if (scc_temp_id not in dict):
                    dict[scc_temp_id] = {'SCC_TEMP_ID': scc_temp_id,
                                         'TEST_ID': test_id,
                                         'LS_DATA_SOURCE': ls_data_source,
                                         'DATE_LOADED': date_loaded,
                                         'LAST_NAME': last_name,
                                         'FIRST_NAME': first_name,
                                         'MIDDLE_NAME': middle_name,
                                         'EMAIL_ADDR': email_addr,
                                         'ADDRESS1': address1,
                                         'CITY': city,
                                         'STATE': state,
                                         'POSTAL': postal,
                                         'COUNTRY': country,
                                         'BIRTHDATE': birthdate,
                                         'HPHONE': hphone,
                                         'STATUS': status,
                                         'CREATED': created,
                                         'LASTUPD': lastupd,
                                         'EMPLID': emplid,
                                         'scores': []}
                    
                dict[scc_temp_id]['scores'].append({'test_component': test_component,'score': score, 'test_dt': test_dt})
                last_scc_temp_id = scc_temp_id
                if line_count % 1000 == 0:
                    print(line_count)
                    conn.commit()
                line_count += 1
            self.process_hash(dict, line_count, fout, conn, skip_insert)
            conn.commit()
            pass # while
        pass # with

    def insert(self, conn, tup):
        insert_q = """
        insert into satstage (scc_temp_id, test_id, ls_data_source, date_loaded, last_name, first_name, middle_name, email_addr, address1, city, state, postal, country, birthdate, phone, status, created, lastupd, emplid,erws, erws_dt, mss, mss_dt, total, total_dt, digest, digparts, digest_nld) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)  on conflict do nothing
        """
        cur = conn.cursor()
        cur.execute(insert_q, tup)

    def update_stage_record(self, conn, tup):
        q = "update satstage set satrecord_id = ? where scc_temp_id = ?"
        cur = conn.cursor()
        res = cur.execute(q, (tup))
        
    def update_satrecord_id(self, conn):
        select_q = """
        select s.scc_temp_id, r.satrecord_id, f.loaddate
        from satstage s, satrecord r, satfile f
        where s.digest_nld = r.digest_nld
        and r.satfile_id = f.satfile_id
        -- and f.loaddate = (select max(f2.loaddate)
        --   from satfile f2, satrecord r2
        --    where r2.satfile_id = f2.satfile_id
        --   and r2.digest_nld = s.digest_nld)
        """
        update_arr = []
        cur = conn.cursor()
        for row in cur.execute(select_q):
            scc_temp_id, satrecord_id, loaddate = row
            #mydb_utils.uga_out(sys.stdout, [scc_temp_id, satrecord_id, loaddate])
            update_arr.append((satrecord_id,scc_temp_id))
        for i, tup in enumerate(update_arr):
            self.update_stage_record(conn,tup)
            if i % 100 == 0:
                conn.commit()
                print(i)
        conn.commit()


            
def main():
    conn = None
    try:
        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        scores_db_file = os.path.join(db_dir, scores_db_name)
        
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")
        
        conn = mydb_utils.sqlite3_connect(scores_db_file)
        score_tables_utils.satstage_table_drop(conn)
        score_tables_utils.satstage_table_create(conn)
        b = Bork()
        b.debug = False
        fout = None
        b.read_ps_sat_stage_query_file(fout, conn)
        b.update_satrecord_id(conn)
        #fout.close
        #b.dump_score_types()
    except Exception as err:
        print("here is your exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
        
    finally:
        if not conn is None:
            conn.close()
        pass
main()

