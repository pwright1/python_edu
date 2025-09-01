
# Copyright Aug 2025, Philip Wright. All rights reserved. 

import re
from pw_utils import string_utils
from pw_utils import biodemo

class AppPsMainSusp:
    def __init__(self, id, slate_person_id, name_last, name_first, name_middle, gender, birth_date, address_address1_mail, address_city_mail, address_address_state_mail, address_address_postal_mail, address_country_mail,email_address_home,phone_number_cell, orgid, nid):
        self.id                             =  id
        self.slate_person_id                =  slate_person_id
        self.name_last                      =  name_last
        self.name_first                     =  name_first
        self.name_middle                    =  name_middle
        self.gender                         =  gender
        self.birth_date                     =  birth_date
        self.address_address1_mail          =  address_address1_mail
        self.address_city_mail              =  address_city_mail
        self.address_address_state_mail     =  address_address_state_mail
        self.address_address_postal_mail    =  address_address_postal_mail
        self.address_country_mail,          =  address_country_mail,
        self.email_address_home             =  email_address_home
        self.phone_number_cell              =  phone_number_cell
        self.orgid                          =  orgid
        self.nid                            = nid
        
    def normLast(self):
        outstring = re.sub(r"\s","",self.name_last)
        outstring = re.sub(r"\'","",outstring)
        return outstring

    def normFirst(self):
        outstring = re.sub(r"\s","",self.name_first)
        outstring = re.sub(r"\'","",outstring)
        return outstring
    
    def fi(self):
        return self.name_first[0:1].upper()

    def biodemo(self):
        
        id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid,phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref = ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "","")

        id1 = self.id
        id2 = self.slate_person_id
        first = self.name_first
        middle = self.name_middle
        last = self.name_last
        sex = self.gender
        dob = self.birth_date
        addr = self.address_address1_mail
        city = self.address_city_mail
        state = self.address_address_state_mail
        postal = self.address_address_postal_mail
        country = self.address_country_mail
        email = self.email_address_home
        phone = self.phone_number_cell
        ceeb = ""
        orgid = self.orgid
        nid = self.nid
        
        app_biodemo = biodemo.Biodemo(id1, id2, first, middle, last, sex, dob, nid, addr, addr2, addr3, addr4, city, state, postal, country, saddr, ceeb, orgid, phone, phone2, phone3, phone4, email, email2, email3, scity, sstate, sname, grade, pref)
        return app_biodemo
    
