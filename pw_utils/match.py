
# Copyright Aug 2025, Philip Wright. All rights reserved. 

from pw_utils import biodemo
from pw_utils import match_result
from pw_utils import match_util

class Match:

    def __init__(self):
        pass

    def compareIt(self,a,b,dosex,dobpenealty):
        rating = 0
        mres = match_result.MatchResult()
        u = match_util.MatchUtil()
        a_a1  = u.parse_address_careful(a.addr.upper())
        b_a1  = u.parse_address_careful(b.addr.upper())
        
        a_sa1  = u.parse_address_careful(a.saddr.upper())
        b_sa1  = u.parse_address_careful(b.saddr.upper())

        afirst = u.normalize(a.first.upper())
        bfirst = u.normalize(b.first.upper())
        apref = u.normalize(a.pref)
        bpref = u.normalize(b.pref)

        if afirst == bfirst and len(afirst) > 0:
            mres.maFirst = 1
        elif afirst == bpref and len(afirst) > 0:
            mres.maFirst = 1
        elif apref == bfirst and len(apref) > 0:
            mres.maFirst = 1
        elif apref == bpref and len(apref) > 0:
            mres.maFirst = 1

        if mres.maFirst == 1:
            rating += 4
        
        if u.normalize(a.last).upper() == u.normalize(b.last).upper() and len(a.last) > 0:
            mres.maLast = 1
            rating += 4
            
        if u.normalize(a.mi()).upper() == u.normalize(b.mi()).upper() and len(a.mi()) > 0:
            mres.maMiddle = 1
            rating += 2

            
        # bork name reversal \/
        if u.normalize(a.last).upper() == u.normalize(b.first).upper() and len(a.last) > 0:
            mres.maRlast = 1
            rating += 1
        if u.normalize(a.first).upper() == u.normalize(b.last).upper() and len(a.first) > 0:
            mres.maRfirst = 1
            rating += 1

        if mres.maRlast == 1 and mres.maRfirst == 1:
            mres.maRname = 1
            rating += 2
         # bork /\
        
       
        if a.fi() == b.fi() and len(a.fi()) > 0:
            mres.maFi = 1
    
        if a.mi() == b.mi() and len(a.mi()) > 0:
            mres.maMi = 1
            rating += 1
        
        #printf("bork ADDR [%s] [%s] parsed  [%s] [%s]\n",a.addr,b.addr,a_a1, b_a1);

        if a_a1 == b_a1 and len(a_a1) > 0:
            mres.maAddr = 1
            rating += 2
    
        if a_sa1 == b_sa1 and len(a_sa1) > 0:
            mres.maSaddr = 1
            rating += 1

        # bork 2025-07-27
        #print(f"a.ceeb {a.ceeb} b.ceeb {b.ceeb}")
        if a.ceeb == b.ceeb and len(a.ceeb) > 0:
            mres.maCeeb = 1
            #rating += 3
    
        if a.orgid == b.orgid and len(a.orgid) > 0:
            mres.maOrgid = 1
            #rating += 3

        if mres.maCeeb == 1 or mres.maOrgid == 1:
            rating += 3
            
        if a.city.upper() == b.city.upper() and len(a.city) > 0:
            mres.maCity = 1
            rating += 1

        if a.scity.upper() == b.scity.upper() and len(a.scity) > 0:
            mres.maScity = 1
            rating += 1

        if a.state.upper() == b.state.upper() and len(a.state) > 0:
            mres.maState = 1
            rating += 1

        if a.sstate.upper() == b.sstate.upper() and len(a.sstate) > 0:
            mres.maSstate = 1
            rating += 1

        if a.sname.upper() == b.sname.upper() and len(a.sname) > 0:
            mres.maSname = 1
            rating += 1

        if a.postal.upper() == b.postal.upper() and len(a.postal) > 0:
            mres.maPostal = 1
            rating += 2

        if a.country.upper() == b.country.upper() and len(a.country) > 0 and not a.country.upper() == "USA":
            mres.maCountry = 1
            rating += 1

        if u.email_compare(a.email, b.email):
            mres.maEmail = 1
            rating += 10

        if mres.maEmail == 0 and (u.email_compare(a.email, b.email2) or  u.email_compare(a.email, b.email3)):
            mres.maPEmail = 1
            rating += 3
    
        #print(f"match phone compare a {a.phone} b {b.phone}")
        #pc1 = u.phone_compare(a.phone, b.phone)
        #pc2 = u.phone_compare(a.phone, b.phone2)
        #pc3 = u.phone_compare(a.phone, b.phone3)
        #pc4 = u.phone_compare(a.phone, b.phone4)
    
        if  u.phone_compare(a.phone, b.phone)  or u.phone_compare(a.phone, b.phone2)  or u.phone_compare(a.phone, b.phone3)  or u.phone_compare(a.phone, b.phone4):
            mres.maPhone = 1
            rating += 4
        #printf("PHONE COMP a %s b %s b2 %s b3 %s b4 %s ==>%d\n",a.phone, b.phone,b.phone2,b.phone3,b.phone4,mres.maPhone)
        
        if a.nid == b.nid and len(a.nid) > 0 and a.nid != "XXXXXXXXX" and a.nid != "999999999":
            mres.maNid = 1
            rating += 6
    
        #if u.fuzzy_nid_match(a.nid, b.nid):
        #    mres.maFnid = 1
        #    rating += 4

        if len(a.dob) > 0 and len(b.dob) > 0:
            if a.dob == b.dob:
                mres.maDob = 1
                rating += 2
            else:
                if dobpenealty:
                    rating -= 2
        
        if dosex:
            if len(a.sex) == 1 and len(b.sex) == 1:
                if a.sex.upper() == b.sex.upper():
                    mres.maSex = 1
            elif  a.sex.upper() == "U" or a.sex.upper() == "-" or b.sex.upper() == "U" or b.sex.upper() == "-":
                mres.maSex = 1
            else:
                mres.maSex = 0
                # do not consider it, another search does
                rating -= 2
    
        mres.maRating = rating
        return mres
    
    #------------------
    def compare1(self,a, b):
        dosex = True
        dobpenealty = True
        return self.compareIt(a,b,dosex,dobpenealty)

    #------------------
    def compare2(self,a,b,dosex):
        dobpenealty = True
        return self.compareIt(a,b,dosex,dobpenealty)
