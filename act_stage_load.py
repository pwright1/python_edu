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

"""
reads from PeopleSoft query .csv file that was an extract of  the ACT stage table data.
used for matching scores in PS. that query DU_UGRD_PW_1501... has a sort order that 
should not be changed for this to work correctly.
stores in a local sqlite3 file
SQLITE3_DB_DIR env var points to dir where db files stored. 
don't open from a network drive, only a local disk. 
"""

class Bork:
    debug = False
    out_hdr = ["SCC_TEMP_ID","TEST_ID","LS_DATA_SOURCE","DATE_LOADED","LAST_NAME","FIRST_NAME","MIDDLE_NAME","EMAIL_ADDR","ADDRESS1","CITY","STATE","POSTAL","COUNTRY","BIRTHDATE","HPHONE","CPHONE","OPHONE","STATUS","CREATED","LASTUPD","EMPLID"]

    score_types = ["ENGL","ENGLS","MATH","MATHS","READ","READS","SCIRE","SCISS","COMP","COMPS","STEM","STEMS","ELA","ELASS","WRS","WRS16","WRSSS","WDIA","WDDS","WDO","WDLC","EW","WRSUB"]

    max_test_age_yrs = 6
    
    def delete(self, conn):
        delete_q = "delete from actstage"
    
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

            scc_temp_id, test_id, ls_data_source, date_loaded, last_name, first_name, middle_name, email_addr, address1, city, state, postal, country, birthdate, hphone, cphone, ophone, status, created, lastupd, emplid = column_arr

            engl, engls, math, maths, read, reads, scire, sciss, comp, comps, stem, stems, ela, elass, wrs, wrs16, wrsss, wdia, wdds, wdo, wdlc, ew, wrsub = scores_column_arr

            engl_sc, engl_dt = engl
            engls_sc, engls_dt = engls
            math_sc, math_dt = math
            maths_sc, maths_dt = maths
            read_sc, read_dt = read
            reads_sc, reads_dt = reads
            scire_sc, scire_dt = scire
            sciss_sc, sciss_dt = sciss
            comp_sc, comp_dt = comp
            comps_sc, comps_dt = comps
            stem_sc, stem_dt = stem
            stems_sc, stems_dt = stems
            ela_sc, ela_dt = ela
            elass_sc, elass_dt = elass
            wrs_sc, wrs_dt = wrs
            wrs16_sc, wrs16_dt = wrs16
            wrsss_sc, wrsss_dt = wrsss
            wdia_sc, wdia_dt = wdia
            wdds_sc, wdds_dt = wdds
            wdo_sc, wdo_dt = wdo
            wdlc_sc, wdlc_dt = wdlc
            ew_sc, ew_dt = ew
            wrsub_sc, wrsub_dt = wrsub
            
            vbirthdate = mydb_utils.date_to_iso(birthdate)
            vdate_loaded = mydb_utils.date_to_iso(date_loaded)
            
            arr = [scc_temp_id, test_id, ls_data_source, vdate_loaded, last_name, first_name, middle_name, email_addr, address1, city, state, postal, country, vbirthdate, hphone, cphone, ophone, status, created, lastupd, emplid, engl_sc, engl_dt, engls_sc, engls_dt, math_sc, math_dt, maths_sc, maths_dt, read_sc, read_dt, reads_sc, reads_dt, scire_sc, scire_dt, sciss_sc, sciss_dt, comp_sc, comp_dt, comps_sc, stem_sc, stem_dt, stems_sc, stems_dt, ela_sc, ela_dt, elass_sc, elass_dt, wrs_sc, wrs_dt, wrs16_sc, wrs16_dt, wrsss_sc, wrsss_dt, wdia_sc, wdia_dt, wdds_sc, wdds_dt, wdo_sc, wdo_dt, wdlc_sc, wdlc_dt, ew_sc, ew_dt, wrsub_sc, wrsub_dt]
            
            vlast = last_name.upper()[0:30].strip()
            vfirst = first_name.upper()[0:30].strip()
            vmi = middle_name.upper()[0:1].strip()
            vdob = ""
            if len(birthdate) == 10:
                vdob = birthdate[6:10] + '-' + birthdate[0:2] + '-' +  birthdate[3:5]
            vemail = email_addr.upper()[0:50].strip()
            vstreet = address1.upper()[0:40].strip()
            vcity = city.upper()[0:30].strip()
            vpostal = ""
            vstate = ""
            if country == "USA":
                vstate = state.upper()[0:2].strip()
                vpostal = postal.upper()[0:5].strip()
            
            digest_test_dt = f"{test_dt[6:10]}{test_dt[0:2]}"
            digparts_enc, hexdigest = act_utils.calc_digest(vdate_loaded,
                                                            self.float_str_to_02_dec(engl_sc),
                                                            engl_dt,
                                                            self.float_str_to_02_dec(engls_sc),
                                                            engls_dt,
                                                            self.float_str_to_02_dec(math_sc),
                                                            math_dt,
                                                            self.float_str_to_02_dec(maths_sc),
                                                            maths_dt,
                                                            self.float_str_to_02_dec(read_sc),
                                                            read_dt,
                                                            self.float_str_to_02_dec(reads_sc),
                                                            reads_dt,
                                                            self.float_str_to_02_dec(scire_sc),
                                                            scire_dt,
                                                            self.float_str_to_02_dec(sciss_sc),
                                                            sciss_dt,
                                                            self.float_str_to_02_dec(comp_sc),
                                                            comp_dt,
                                                            self.float_str_to_02_dec(comps_sc),
                                                            vlast,
                                                            vfirst,
                                                            vmi,
                                                            vdob,
                                                            vemail,
                                                            vstreet,
                                                            vcity,
                                                            vstate,
                                                            vpostal)

            hexdigest_nld = act_utils.calc_digest_nld(self.float_str_to_02_dec(engl_sc),
                                                            engl_dt,
                                                            self.float_str_to_02_dec(engls_sc),
                                                            engls_dt,
                                                            self.float_str_to_02_dec(math_sc),
                                                            math_dt,
                                                            self.float_str_to_02_dec(maths_sc),
                                                            maths_dt,
                                                            self.float_str_to_02_dec(read_sc),
                                                            read_dt,
                                                            self.float_str_to_02_dec(reads_sc),
                                                            reads_dt,
                                                            self.float_str_to_02_dec(scire_sc),
                                                            scire_dt,
                                                            self.float_str_to_02_dec(sciss_sc),
                                                            sciss_dt,
                                                            self.float_str_to_02_dec(comp_sc),
                                                            comp_dt,
                                                            self.float_str_to_02_dec(comps_sc),
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
        
    def read_ps_act_stage_query_file(self, fout, conn):
        
        dict = {}
        now = date.today()
        expected_fields = 24
        if len(sys.argv[1:]) == 0:
            print("use _ filename.csv")
            sys.exit()
        fname = sys.argv[1]
        if len(fname) < 27:
            raise RuntimeError ("File name needs to have _1501_ACT in it to load")

        match_arr = re.findall(r"_1501_ACT",fname)
        if len(match_arr) != 1 or match_arr[0] != "_1501_ACT":
            raise RuntimeError ("File name needs to have _1501_ACT in it to load")
        
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
                (scc_temp_id,test_id,test_component,test_dt,ls_data_source,score,date_loaded,last_name,first_name,middle_name,email_addr,address1,city,state,postal,country,birthdate,hphone,cphone,ophone,status,created,lastupd,emplid) = fields
                
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
                                         'CPHONE': cphone,
                                         'OPHONE': ophone,
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
        insert into actstage (scc_temp_id, test_id, ls_data_source, date_loaded, last_name, first_name, middle_name, email_addr, address1, city, state, postal, country, birthdate, hphone, cphone, ophone, status, created, lastupd, emplid, engl, engl_dt, engls, engls_dt, math, math_dt, maths, maths_dt, read, read_dt, reads, reads_dt, scire, scire_dt, sciss, sciss_dt, comp, comp_dt, comps, stem, stem_dt, stems, stems_dt, ela, ela_dt, elass, elass_dt, wrs, wrs_dt, wrs16, wrs16_dt, wrsss, wrsss_dt, wdia, wdia_dt, wdds, wdds_dt, wdo, wdo_dt, wdlc, wdlc_dt, ew, ew_dt, wrsub, wrsub_dt, digest, digparts, digest_nld) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        on conflict do nothing
    """
        cur = conn.cursor()
        cur.execute(insert_q, tup)

    def update_stage_record(self, conn, tup):
        q = "update actstage set actrecord_id = ? where scc_temp_id = ?"
        cur = conn.cursor()
        res = cur.execute(q, (tup))
        
    def update_actrecord_id(self, conn):
        select_q = """
        select s.scc_temp_id, r.actrecord_id, f.loaddate
        from actstage s, actrecord r, actfile f
        where s.digest_nld = r.digest_nld
        and r.actfile_id = f.actfile_id
        -- and f.loaddate = (select max(f2.loaddate)
        --    from actfile f2, actrecord r2
        --   where r2.actfile_id = f2.actfile_id
        --   and r2.digest_nld = s.digest_nld)
        """
        update_arr = []
        cur = conn.cursor()
        for row in cur.execute(select_q):
            scc_temp_id, actrecord_id, loaddate = row
            #mydb_utils.uga_out(sys.stdout, [scc_temp_id, actrecord_id, loaddate])
            update_arr.append((actrecord_id,scc_temp_id))
        for i, tup in enumerate(update_arr):
            self.update_stage_record(conn,tup)
            if i % 1000 == 0:
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
        score_tables_utils.actstage_table_drop(conn)
        score_tables_utils.actstage_table_create(conn)
        b = Bork()
        fout = None
        b.read_ps_act_stage_query_file(fout, conn)
        b.update_actrecord_id(conn)
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

