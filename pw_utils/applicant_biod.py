
# Copyright Aug 2025, Philip Wright. All rights reserved. 

from pw_utils import biodemo

class ApplicantBiod:
    def __init__(self,slateid, emplid, appno, nplan, admittype, admitterm, first, middle, last, prefname, suffix, sex, birthdate, email, fatheremail, motheremail, cellphone, homephone, fatherphone, motherphone, maddress1, maddress2, maddress3, maddress4, mcity, mstate, mpostal, mcountry, mcountryde, haddress1, haddress2, haddress3, haddress4, hcity, hstate, hpostal, hcountry, hcountryde, sname, saddress1, saddress2, scity, sstate, spostal, scountry, orgid, ceeb, test_consider, nid=""):
        self.slateid       = slateid
        self.emplid        = emplid
        self.appno         = appno
        self.nplan         = nplan
        self.admittype     = admittype
        self.admitterm     = admitterm
        self.first         = first
        self.middle        = middle
        self.last          = last
        self.prefname      = prefname
        self.suffix        = suffix
        self.sex           = sex
        self.birthdate     = birthdate
        self.email         = email
        self.fatheremail   = fatheremail
        self.motheremail   = motheremail
        self.cellphone     = cellphone
        self.homephone     = homephone
        self.fatherphone   = fatherphone
        self.motherphone   = motherphone
        self.maddress1     = maddress1
        self.maddress2     = maddress2
        self.maddress3     = maddress3
        self.maddress4     = maddress4
        self.mcity         = mcity
        self.mstate        = mstate
        self.mpostal       = mpostal
        self.mcountry      = mcountry
        self.mcountryde    = mcountryde
        self.haddress1     = haddress1
        self.haddress2     = haddress2
        self.haddress3     = haddress3
        self.haddress4     = haddress4
        self.hcity         = hcity
        self.hstate        = hstate
        self.hpostal       = hpostal
        self.hcountry      = hcountry
        self.hcountryde    = hcountryde
        self.sname         = sname
        self.saddress1     = saddress1
        self.saddress2     = saddress2
        self.scity         = scity
        self.sstate        = sstate
        self.spostal       = spostal
        self.scountry      = scountry
        self.ceeb          = ceeb
        self.orgid         = orgid
        self.test_consuder = test_consider
        self.nid           = nid
        
    def homeBiodemo(self):
        id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref = ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "","")
        
        id1 = self.slateid
        id2 = self.emplid
        first = self.first
        middle = self.middle
        last = self.last
        pref = self.prefname
        sex = self.sex
        dob = self.birthdate
        addr = self.haddress1
        addr2 = self.haddress2
        addr3 = self.haddress3
        addr4 = self.haddress4
        city = self.hcity
        state = self.hstate
        postal = self.hpostal
        country = self.hcountry
        saddr = self.saddress1
        #atp = self.orgid
        ceeb = self.ceeb
        orgid = self.orgid
        phone = self.cellphone
        phone2 = self.homephone
        phone3 = self.fatherphone
        phone4 = self.motherphone
        email = self.email
        email2 = self.fatheremail
        email3 = self.motheremail
        scity = self.scity
        sstate = self.sstate
        sname = self.sname
        nid = self.nid
        
        h_biodemo = biodemo.Biodemo(id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref)
        return h_biodemo
        
    def mailBiodemo(self):
        id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref = ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "","")
        
        id1 = self.slateid
        id2 = self.emplid
        first = self.first
        middle = self.middle
        last = self.last
        pref = self.prefname
        sex = self.sex
        dob = self.birthdate
        addr = self.maddress1
        addr2 = self.maddress2
        addr3 = self.maddress3
        addr4 = self.maddress4
        city = self.mcity
        state = self.mstate
        postal = self.mpostal
        country = self.mcountry
        saddr = self.saddress1
        #atp = self.orgid
        ceeb = self.ceeb
        orgid = self.orgid
        phone = self.cellphone
        phone2 = self.homephone
        phone3 = self.fatherphone
        phone4 = self.motherphone
        email = self.email
        email2 = self.fatheremail
        email3 = self.motheremail
        scity = self.scity
        sstate = self.sstate
        sname = self.sname
        nid = self.nid
        
        m_biodemo = biodemo.Biodemo(id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref)
        return m_biodemo
        
