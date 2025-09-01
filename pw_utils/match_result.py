
# Copyright Aug 2025, Philip Wright. All rights reserved. 

class MatchResult:

    def __init__(self):
        self.maFirst = 0
        self.maMiddle = 0
        self.maLast = 0
        self.maSex = 0
        self.maDob = 0
        self.maNid = 0
        self.maFnid = 0
        self.maAddr = 0
        self.maCity = 0
        self.maState = 0
        self.maPostal = 0
        self.maCountry = 0
        self.maSaddr = 0
        #self.maAtp = 0
        self.maCeeb = 0
        self.maOrgid = 0
        self.maPhone = 0
        self.maEmail = 0
        self.maPEmail = 0
        self.maMi = 0
        self.maFi = 0
        self.maScity = 0
        self.maSstate = 0
        self.maSname = 0
        self.maRating = 0
        self.maRname = 0
        self.maRfirst = 0
        self.maRlast = 0
    
    def maApe(self):
        if self.maPhone == 1 or self.maEmail == 1 or self.maAddr == 1:
            return 1
        return 0
    
    def maAp(self):
        if self.maPhone == 1 or self.maAddr == 1:
            return 1
        return 0

    def isAutoMatch(self):
        if self.maApe() == 1 and self.maDob == 1 and self.maFirst == 1 and self.maLast == 1:
            return True
        return False
