
# Copyright Sep 2025, Philip Wright. All rights reserved. 

import hashlib
from pw_utils import string_utils
from pw_utils import toefl_utils

class ToeflRecord:
    debug = False
    
    def __init__(self):
        self.clear()
        pass

    def mysub(self, line, ind1, ind2):
        a = ind1-1
        b = ind2
        return line[a:b]
    
    def clear(self):
        self.toefl_file_id = 0
        self.fileline = 0
        self.icode = ""
        self.dcode = ""
        self.ran = ""
        self.last = ""
        self.first = ""
        self.middle = ""
        self.a1 = ""
        self.a2 = ""
        self.a3 = ""
        self.a4 = ""
        self.city = ""
        self.state = ""
        self.country = ""
        self.countryde = ""
        self.postal = ""
        self.nat_country = ""
        self.nat_countryde = ""
        self.nat_lang = ""
        self.nat_langde = ""
        self.dob = ""
        self.gendernum = ""
        self.admin_date = ""
        self.test_ctr_code = ""
        self.test_type = ""
        self.listen_ind = ""
        self.speak_ind = ""
        self.ibt_listening = ""
        self.ibt_reading = ""
        self.ibt_speaking = ""
        self.ibt_writing = ""
        self.ibt_total = ""
        self.pb_reason = ""
        self.pb_degree = ""
        self.pb_sec1 = ""
        self.pb_sec2 = ""
        self.pb_sec3 = ""
        self.pb_conv_twe = ""
        self.pb_total = ""
        self.pb_year = ""
        self.pb_times_taken = ""
        self.pb_nrsp_offt = ""
        self.email = ""
        self.ibt_rpdt_test_ctr_code = ""
        self.test_ctr_country = ""
        self.identification_type = ""
        self.id_number = ""
        self.id_country = ""
        self.rpdt_listening = ""
        self.rpdt_reading = ""
        self.rpdt_writing = ""
        self.dob_ex = ""
        self.gender_ex = ""
        self.mb_ibt_l = ""
        self.mb_ibt_ltd = ""
        self.mb_ibt_r = ""
        self.mb_ibt_rtd = ""
        self.mb_ibt_w = ""
        self.mb_ibt_wtd = ""
        self.mb_ibt_s = ""
        self.mb_ibt_std = ""
        self.mb_ibt_t = ""
        self.mb_ibt_taod = ""
        self.ess_l = ""
        self.ess_r = ""
        self.ess_w = ""
        self.ess_s = ""
        self.ess_tbs = ""
        self.ess_mb_l = ""
        self.ess_mb_ltd = ""
        self.ess_mb_r = ""
        self.ess_mb_rtd = ""
        self.ess_mb_w = ""
        self.ess_mb_wtd = ""
        self.ess_mb_s = ""
        self.ess_mb_std = ""
        self.ess_mb_tbs = ""
        self.ess_mb_aod = ""
        self.ess_cefr_l = ""
        self.ess_cefr_r = ""
        self.ess_cefr_w = ""
        self.ess_cefr_s = ""
        self.ess_cefr_tbs = ""
        self.ess_found_sc = ""
        self.ess_found_vk = ""
        self.digparts = ""
        self.digest_nld = ""
       
    def header(self):
        # make an array out of it - ruby %w[]
        hdr = string_utils.pct_w("toefl_file_id fileline icode dcode ran last first middle a1 a2 a3 a4 city state country countryde postal nat_country nat_countryde nat_lang nat_langde dob gendernum admin_date test_ctr_code test_type listen_ind speak_ind ibt_listening ibt_reading ibt_speaking ibt_writing ibt_total pb_reason pb_degree pb_sec1 pb_sec2 pb_sec3 pb_conv_twe pb_total pb_year pb_times_taken pb_nrsp_offt email ibt_rpdt_test_ctr_code test_ctr_country identification_type id_number id_country rpdt_listening rpdt_reading rpdt_writing dob_ex gender_ex mb_ibt_l mb_ibt_ltd mb_ibt_r mb_ibt_rtd mb_ibt_w mb_ibt_wtd mb_ibt_s mb_ibt_std mb_ibt_t mb_ibt_taod ess_l ess_r ess_w ess_s ess_tbs ess_mb_l ess_mb_ltd ess_mb_r ess_mb_rtd ess_mb_w ess_mb_wtd ess_mb_s ess_mb_std ess_mb_tbs ess_mb_aod ess_cefr_l ess_cefr_r ess_cefr_w ess_cefr_s ess_cefr_tbs ess_found_sc ess_found_vk digparts digest_nld")
        return hdr

    def toArr(self):
        arr = [self.toefl_file_id, self.fileline, self.icode, self.dcode, self.ran, self.last, self.first, self.middle, self.a1, self.a2, self.a3, self.a4, self.city, self.state, self.country, self.countryde, self.postal, self.nat_country, self.nat_countryde, self.nat_lang, self.nat_langde, self.dob, self.gendernum, self.admin_date, self.test_ctr_code, self.test_type, self.listen_ind, self.speak_ind, self.ibt_listening, self.ibt_reading, self.ibt_speaking, self.ibt_writing, self.ibt_total, self.pb_reason, self.pb_degree, self.pb_sec1, self.pb_sec2, self.pb_sec3, self.pb_conv_twe, self.pb_total, self.pb_year, self.pb_times_taken, self.pb_nrsp_offt, self.email, self.ibt_rpdt_test_ctr_code, self.test_ctr_country, self.identification_type, self.id_number, self.id_country, self.rpdt_listening, self.rpdt_reading, self.rpdt_writing, self.dob_ex, self.gender_ex, self.mb_ibt_l, self.mb_ibt_ltd, self.mb_ibt_r, self.mb_ibt_rtd, self.mb_ibt_w, self.mb_ibt_wtd, self.mb_ibt_s, self.mb_ibt_std, self.mb_ibt_t,  self.mb_ibt_taod, self.ess_l, self.ess_r, self.ess_w, self.ess_s, self.ess_tbs, self.ess_mb_l, self.ess_mb_ltd, self.ess_mb_r, self.ess_mb_rtd, self.ess_mb_w, self.ess_mb_wtd, self.ess_mb_s, self.ess_mb_std, self.ess_mb_tbs, self.ess_mb_aod, self.ess_cefr_l, self.ess_cefr_r, self.ess_cefr_w, self.ess_cefr_s, self.ess_cefr_tbs, self.ess_found_sc, self.ess_found_vk, self.digparts, self.digest_nld]
        return arr

    def insert_toefl_sql(self):
        harr = self.header()
        vals = "?," * (len(harr)-1)
        vals += "?"
        q = f"insert into toefl_record ({','.join(harr)}) values ({vals})"
        return q
    
    
    def set_fields_from_line(self, line, line_count, toefl_file_id, fname):
        # line len includes newline
        if len(line) != 1601 and len(line) != 901:
            print(f"error {fname} {line_count}")
            raise RuntimeError(f"ferror, toefl line length {len(line)-1} not new format 1600 or old 900")

        self.toefl_file_id = toefl_file_id
        self.fileline = line_count
        self.icode = self.mysub(line,1,4).strip()
        self.dcode = self.mysub(line,10,11).strip()
        self.ran = self.mysub(line,27,42).strip()
        self.last = self.mysub(line,43,72).strip()
        self.first = self.mysub(line,73,102).strip()
        self.middle = self.mysub(line,103,132).strip()
        self.a1 = self.mysub(line,133,187).strip()
        self.a2 = self.mysub(line,188,242).strip()
        self.a3 = self.mysub(line,243,297).strip()
        self.a4 = self.mysub(line,298,352).strip()
        self.city = self.mysub(line,353,382).strip()
        self.state = self.mysub(line,383,388).strip()
        self.country = self.mysub(line,389,391).strip()
        self.countryde = self.mysub(line,392,431).strip()
        self.postal = self.mysub(line,432,443).strip()
        # not storing these
        #self.nat_country = self.mysub(line,444,446).strip()
        #self.nat_countryde = self.mysub(line,447,486).strip()
        #self.nat_lang = self.mysub(line,487,489).strip()
        #self.nat_langde = self.mysub(line,490,529).strip()
        self.dob = self.mysub(line,530,537).strip()
        self.gendernum = self.mysub(line,538,538).strip()
        self.admin_date = self.mysub(line,539,546).strip()
        self.test_ctr_code = self.mysub(line,547,551).strip()
        self.test_type  = self.mysub(line,556,556).strip()
        self.listen_ind = self.mysub(line,557,557).strip()
        self.speak_ind = self.mysub(line,558,558).strip()
        self.ibt_listening = self.mysub(line,559,560).strip()
        self.ibt_reading = self.mysub(line,561,562).strip()
        self.ibt_speaking = self.mysub(line,563,564).strip()
        self.ibt_writing = self.mysub(line,565,566).strip()
        self.ibt_total = self.mysub(line,567,569).strip()
        self.pb_reason = self.mysub(line,572,572).strip()
        self.pb_degree = self.mysub(line,573,573).strip()
        self.pb_sec1 = self.mysub(line,574,575).strip()
        self.pb_sec2 = self.mysub(line,576,577).strip()
        self.pb_sec3 = self.mysub(line,578,579).strip()
        self.pb_conv_twe = self.mysub(line,580,581).strip()
        self.pb_total = self.mysub(line,582,584).strip()
        self.pb_year = self.mysub(line,585,588).strip()
        self.pb_times_taken = self.mysub(line,589,589).strip()
        self.pb_nrsp_offt = self.mysub(line,590,590).strip()
        self.email = self.mysub(line,591,665).strip()
        self.ibt_rbpt_test_ctr_code = self.mysub(line,666,680).strip()
        self.test_ctr_country = self.mysub(line,681,716).strip()
        self.identification_type = self.mysub(line,717,731).strip()
        # not doing this one...
        #self.id_number = self.mysub(line,732,756).strip()
        self.id_country = self.mysub(line,757,766).strip()
        self.rpdt_listening = self.mysub(line,767,768).strip()
        self.rpdt_reading = self.mysub(line,769,770).strip()
        self.rpdt_writing = self.mysub(line,771,772).strip()

        self.dob_ex = ""
        if len(self.dob) == 8:
            self.dob_ex = self.dob[0:4] + "-" + self.dob[4:6] + "-" + self.dob[6:8]

        self.gender_ex = ""
        if self.gendernum == "1":
            self.gender_ex = "M"
        elif self.gendernum == "2":
            self.gender_ex = "F"
        elif self.gendernum == "0":
            self.gender_ex = "U"
            
        if len(line) == 1600:
            self.mb_ibt_l = self.mysub(line,773,774).strip()  
            self.mb_ibt_ltd = self.mysub(line,775,782).strip()  
            self.mb_ibt_r = self.mysub(line,783,784).strip()  
            self.mb_ibt_rtd = self.mysub(line,785,792).strip()  
            self.mb_ibt_w = self.mysub(line,793,794).strip()  
            self.mb_ibt_wtd = self.mysub(line,795,802).strip()  
            self.mb_ibt_s = self.mysub(line,803,804).strip()  
            self.mb_ibt_std = self.mysub(line,805,812).strip()  
            self.mb_ibt_t = self.mysub(line,813,815).strip()  
            self.mb_ibt_taod = self.mysub(line,816,823).strip()  
            self.ess_l = self.mysub(line,1201,1202).strip()  
            self.ess_r = self.mysub(line,1203,1204).strip()  
            self.ess_w = self.mysub(line,1205,1206).strip()  
            self.ess_s = self.mysub(line,1207,1208).strip()  
            self.ess_tbs = self.mysub(line,1209,1212).strip()  
            self.ess_mb_l = self.mysub(line,1213,1214).strip()  
            self.ess_mb_ltd = self.mysub(line,1215,1222).strip()  
            self.ess_mb_r = self.mysub(line,1223,1224).strip()  
            self.ess_mb_rtd = self.mysub(line,1225,1232).strip()  
            self.ess_mb_w = self.mysub(line,1233,1234).strip()  
            self.ess_mb_wtd = self.mysub(line,1235,1242).strip()  
            self.ess_mb_s = self.mysub(line,1243,1244).strip()  
            self.ess_mb_std = self.mysub(line,1245,1252).strip()  
            self.ess_mb_tbs = self.mysub(line,1253,1256).strip()  
            self.ess_mb_aod = self.mysub(line,1257,1264).strip()  
            self.ess_cefr_l = self.mysub(line,1265,1266).strip()  
            self.ess_cefr_r = self.mysub(line,1267,1268).strip()  
            self.ess_cefr_w = self.mysub(line,1269,1270).strip()  
            self.ess_cefr_s = self.mysub(line,1271,1272).strip()  
            self.ess_cefr_tbs = self.mysub(line,1273,1274).strip()  
            self.ess_found_sc = self.mysub(line,1275,1276).strip()  
            self.ess_found_vk = self.mysub(line,1277,1278).strip()
        else:
            self.mb_ibt_l = ""
            self.mb_ibt_ltd = ""
            self.mb_ibt_r = ""
            self.mb_ibt_rtd = ""
            self.mb_ibt_w = ""
            self.mb_ibt_wtd = ""
            self.mb_ibt_s = ""
            self.mb_ibt_std = ""
            self.mb_ibt_t = ""
            self.mb_ibt_taod = ""
            self.ess_l = ""
            self.ess_r = ""
            self.ess_w = ""
            self.ess_s = ""
            self.ess_tbs = ""
            self.ess_mb_l = ""
            self.ess_mb_ltd = ""
            self.ess_mb_r = ""
            self.ess_mb_rtd = ""
            self.ess_mb_w = ""
            self.ess_mb_wtd = ""
            self.ess_mb_s = ""
            self.ess_mb_std = ""
            self.ess_mb_tbs = ""
            self.ess_mb_aod = ""
            self.ess_cefr_l = ""
            self.ess_cefr_r = ""
            self.ess_cefr_w = ""
            self.ess_cefr_s = ""
            self.ess_cefr_tbs = ""
            self.ess_found_sc = ""
            self.ess_found_vk = ""

        
        digparts, hexdigest = toefl_utils.calc_digest_nld(self.ran, self.last, self.first, self.middle, self.dob_ex, self.email, self.city, self.country)
        self.digparts = digparts
        self.digest_nld = hexdigest
        
        
