
# Copyright Aug 2025, Philip Wright. All rights reserved. 

class Biodemo:
    def __init__(self, id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref):
        self.id1 = id1
        self.id2 = id2
        self.first = first;
        self.middle = middle
        self.last = last
        self.sex = sex
        self.dob = dob
        self.nid = nid
        self.addr = addr
        self.addr2 = addr2
        self.addr3 = addr3
        self.addr4 = addr4
        self.city = city
        self.state = state
        self.postal = postal
        self.country = country
        self.saddr = saddr
        #self.atp = atp
        self.ceeb = ceeb
        self.orgid = orgid
        self.phone =  phone
        self.phone2 = phone2
        self.phone3 = phone3
        self.phone4 = phone4
        self.email = email
        self.email2 = email2
        self.email3 = email3
        self.scity = scity
        self.sstate = sstate
        self.sname = sname
        self.grade = grade
        self.pref = pref

    def mi(self):
        if len(self.middle) > 0:
            return self.middle[0:1].upper()
        return ""

    def fi(self):
        if len(self.first) > 0:
            return self.first[0:1].upper()
        return ""
