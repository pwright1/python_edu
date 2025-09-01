
# Copyright Aug 2025, Philip Wright. All rights reserved. 

from pw_utils import biodemo

class ApoolLast:
    def __init__(self,id,emplid,appno,last,first,middle,pref,suffix,dob,nid,a1,a2,city,state,postal,country,phone,email,sex, org):
        self.id            = id
        self.emplid        = emplid
        self.appno         = appno
        self.last          = last
        self.first         = first
        self.middle        = middle
        self.pref          = pref
        self.suffix        = suffix
        self.dob           = dob
        self.nid           = nid
        self.maddress1     = a1
        self.maddress2     = a2
        self.mcity         = city
        self.mstate        = state
        self.mpostal       = postal
        self.mcountry      = country
        self.phone         = phone
        self.email         = email
        self.sex           = sex
        self.ceeb          = org
        self.atp           = org
        # fields we don't have stored but expected to be there
        self.scountry      = ""
        self.slateid       = ""
        self.nplan         = ""
        
    def homeBiodemo(self):
        id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, atp, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref = ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        
        id1 = self.id
        id2 = self.emplid
        first = self.first
        middle = self.middle
        last = self.last
        pref = self.pref
        sex = self.sex
        nid = self.nid
        dob = self.dob
        addr = self.maddress1
        addr2 = self.maddress2
        city = self.mcity
        state = self.mstate
        postal = self.mpostal
        country = self.mcountry
        phone = self.phone
        email = self.email
        atp = self.atp
        
        h_biodemo = biodemo.Biodemo(id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, atp, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref)
        return h_biodemo
        
    def mailBiodemo(self):
        id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, atp, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref = ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        
        id1 = self.id
        id2 = self.emplid
        first = self.first
        middle = self.middle
        last = self.last
        pref = self.pref
        sex = self.sex
        dob = self.dob
        nid = self.nid
        addr = self.maddress1
        addr2 = self.maddress2
        city = self.mcity
        state = self.mstate
        postal = self.mpostal
        country = self.mcountry
        phone = self.phone
        email = self.email
        atp = self.atp
        
        m_biodemo = biodemo.Biodemo(id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, atp, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref)
        return m_biodemo
        
