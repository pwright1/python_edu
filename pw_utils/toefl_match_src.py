
# Copyright Aug 2025, Philip Wright. All rights reserved. 

from pw_utils import biodemo

class ToeflMatchSrc:
    def __init__(self,toefl_record_id, scc_temp_id, last, first, middle, sex, dob, a1, a2, a3, a4, city, state, postal, country, email):

        self.toefl_record_id  = toefl_record_id
        self.scc_temp_id   =   scc_temp_id
        self.last          =   last
        self.first         =   first
        self.middle        =   middle
        self.sex           =   sex
        self.dob           =   dob
        self.a1            =   a1
        self.a2            =   a2
        self.a3            =   a3
        self.a4            =   a4
        self.city          =   city
        self.state         =   state
        self.postal        =   postal
        self.country       =   country
        self.email         =   email
        
    def to_arr(self):
        return [toefl_record_id, scc_temp_id, last, first, middle, sex, dob, a1, a2, a3, a4, city, state, postal, country, email]
    
    def biodemo(self):
        id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref = ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "","")

        id1 = self.toefl_record_id
        id2 = self.scc_temp_id
        first = self.first
        middle = self.middle
        last = self.last
        sex = self.sex
        dob = self.dob
        addr = self.a1
        addr2 = self.a2
        addr3 = self.a3
        addr4 = self.a4
        city = self.city
        state = self.state
        postal = self.postal
        country = self.country
        email = self.email

        toefl_biodemo = biodemo.Biodemo(id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref)

        return toefl_biodemo
