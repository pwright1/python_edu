
# Copyright Aug 2025, Philip Wright. All rights reserved. 

from pw_utils import biodemo

class SatMatchSrc:
    def __init__(self,  satrecord_id, scc_temp_id, last, first, mi, sex, dob, addr1, city, state, postal, country, phone, email, hscode, hsname, loaddate, cbsid):

        self.satrecord_id  =   satrecord_id
        self.scc_temp_id   =   scc_temp_id
        self.last          =   last
        self.first         =   first
        self.mi            =   mi
        self.sex           =   sex
        self.dob           =   dob
        self.addr1         =   addr1
        self.city          =   city
        self.state         =   state
        self.postal        =   postal
        self.country       =   country
        self.phone         =   phone
        self.email         =   email
        self.hscode        =   hscode
        self.hsname        =   hsname
        self.loaddate      =   loaddate
        self.cbsid         =   cbsid
        
    def to_arr(self):
        return [satrecord_id, scc_temp_id, last, first, mi, sex, dob, addr1, city, state, postal, country, phone, email, hscode, hsname, loaddate, cbsid]
    
    def biodemo(self):
        id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref = ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "","")

        id1 = self.satrecord_id
        id2 = self.scc_temp_id
        first = self.first
        middle = self.mi
        last = self.last
        sex = self.sex
        dob = self.dob
        addr = self.addr1
        city = self.city
        state = self.state
        postal = self.postal
        country = self.country
        phone = self.phone
        ceeb = self.hscode
        sname = self.hsname
        orgid = ""
        email = self.email
        nid = self.cbsid  # store it here, as its not used

        sat_biodemo = biodemo.Biodemo(id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref)

        return sat_biodemo
