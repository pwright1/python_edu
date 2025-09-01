

# Copyright Aug 2025, Philip Wright. All rights reserved. 

from pw_utils import biodemo

class ActMatchSrc:
    def __init__(self,  actrecord_id, scc_temp_id, last, first, mi, gender, edob, street, city, state, zip5, hscode, email, loaddate, actid):

        self.actrecord_id  =   actrecord_id
        self.scc_temp_id   =   scc_temp_id
        self.last          =   last
        self.first         =   first
        self.mi            =   mi
        self.gender        =   gender
        self.edob          =   edob
        self.street        =   street
        self.city          =   city
        self.state         =   state
        self.zip5          =   zip5
        self.hscode        =   hscode
        self.email         =   email
        self.loaddate      =   loaddate
        self.actid         =   actid
        
    def to_arr(self):
        return [actrecord_id, scc_temp_id, last, first, mi, gender, edob, street, city, state, zip5, hscode, email, loaddate, actid]
    
    def biodemo(self):
        id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref = ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "","")

        id1 = self.actrecord_id
        id2 = self.scc_temp_id
        first = self.first
        middle = self.mi
        last = self.last
        sex = self.gender
        dob = self.edob
        addr = self.street
        city = self.city
        state = self.state
        postal = self.zip5
        ceeb = self.hscode
        orgid = ""
        email = self.email

        act_biodemo = biodemo.Biodemo(id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref)

        return act_biodemo
