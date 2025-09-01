
# Copyright Aug 2025, Philip Wright. All rights reserved. 

from pw_utils import string_utils

class AppMatchResult:

    def __init__(self, sa, susp, mr, blank_ssn):
        ahome = sa.homeBiodemo()
        amail = sa.mailBiodemo()
        bsusp = susp.biodemo()

        self.setVals(bsusp.id1,
                     sa.slateid,
                     sa.emplid,
                     ahome.last,
                     bsusp.last,
                     ahome.first,
                     ahome.pref,
                     bsusp.first,
                     ahome.middle,
                     bsusp.mi(),
                     ahome.sex,
                     bsusp.sex,
                     ahome.dob,
                     bsusp.dob,
                     ahome.nid,
                     bsusp.nid,
                     amail.addr,
                     ahome.addr,
                     ahome.saddr,
                     bsusp.addr,
                     amail.city,
                     ahome.city,
                     ahome.scity,
                     bsusp.city,
                     amail.state,
                     ahome.state,
                     ahome.sstate,
                     bsusp.state,
                     amail.postal,
                     ahome.postal,
                     bsusp.postal,
                     amail.country,
                     ahome.country,
                     sa.scountry,
                     bsusp.country,
                     ahome.phone,
                     ahome.phone2,
                     ahome.phone3,
                     ahome.phone4,
                     bsusp.phone,
                     ahome.email,
                     ahome.email2,
                     ahome.email3,
                     bsusp.email,
                     sa.ceeb,
                     bsusp.ceeb,
                     sa.orgid,
                     bsusp.orgid,
                     f"{mr.maRating:02d}",
                     f"{mr.maApe():d}",
                     f"{mr.maAddr:d}",
                     f"{mr.maDob:d}",
                     f"{mr.maNid:d}",
                     f"{mr.maPhone:d}",
                     f"{mr.maCeeb:d}",
                     f"{mr.maOrgid:d}",
                     f"{mr.maPostal:d}",
                     f"{mr.maFirst:d}",
                     f"{mr.maEmail:d}",
                     sa.nplan,
                     sa.appno,
                     f"{mr.maLast:d}",
                     blank_ssn)
        pass # def init

    def header():
        hdr_str = """ID1_ SLID_ EMPLID_ ANO_ PS_LAST_ LD_LAST_ PS_FIRST_ PS_PREF_ LD_FIRST_ PS_MID_ LMI_ AX_ LX_ PS_DOB_ LD_DOB_ PS_NID LD_NID PS_MA1_ PS_HA1_ PS_SA1_ LD_ADDR_ PS_MCITY_ PS_HCITY_ PS_SCITY_ LD_CITY_ PS_MSTATE_ PS_HSTATE_ PS_SSTATE_ LD_STATE_ PS_MPOSTAL_ PS_HPOSTAL_ LD_POSTAL_ PS_MCO_ PS_HCO_ PS_SCO_ LD_CO_ HPHONE_ CPHONE_ MOPHONE_ FAPHONE_ LD_PHONE_ EMAIL_ MOEMAIL_ FAEMAIL_ LD_EMAIL_ PS_CEEB_ LD_CEEB_ PS_ORG_ LD_ORG_ RAT_ APE_ ADR_ DOB_ NID_ PHO_ CEEB_ ORG_ POS_ FN_ EM_ NPL_ LN_"""
        return string_utils.pct_w(hdr_str)
    
    def setVals(self, id1,id2,emplid,ap_last,susp_last,ap_first,ap_pref,susp_first,ap_mid,smi,ax,sx,ap_dob,susp_dob,ap_nid, susp_nid, ap_ma1,ap_ha1,ap_sa1,susp_addr,ap_mcity,ap_hcity,ap_scity,susp_city,ap_mstate,ap_hstate,ap_sstate,susp_state,ap_mpostal,ap_hpostal,susp_postal,ap_mco,ap_hco,ap_sco,susp_co,hphone,cphone,mophone,faphone,susp_phone,email,moemail,faemail,susp_email,ap_ceeb,susp_ceeb,ap_orgid,susp_orgid,rat,ape,adr,dob,nid,pho,ceeb,orgid,pos,fn,em,npl,ano,ln, blank_ssn):

        self.id1= id1
        self.id2 = id2
        self.emplid = emplid
        self.ap_last = ap_last
        self.susp_last = susp_last
        self.ap_first = ap_first
        self.ap_pref = ap_pref
        self.susp_first = susp_first
        self.ap_mid = ap_mid
        self.smi = smi
        self.ax = ax
        self.sx = sx
        self.ap_dob = ap_dob
        self.susp_dob = susp_dob
        if blank_ssn:
            self.ap_nid = ""
            self.susp_nid = ""
        else:
            self.ap_nid = ap_nid
            self.susp_nid = susp_nid
        self.ap_ma1 = ap_ma1
        self.ap_ha1 = ap_ha1
        self.ap_sa1 = ap_sa1
        self.susp_addr = susp_addr
        self.ap_mcity = ap_mcity
        self.ap_hcity = ap_hcity
        self.ap_scity = ap_scity
        self.susp_city = susp_city
        self.ap_mstate = ap_mstate
        self.ap_hstate = ap_hstate
        self.ap_sstate = ap_sstate
        self.susp_state = susp_state
        self.ap_mpostal = ap_mpostal
        self.ap_hpostal = ap_hpostal
        self.susp_postal = susp_postal
        self.ap_mco = ap_mco
        self.ap_hco = ap_hco
        self.ap_sco = ap_sco
        self.susp_co = susp_co
        self.hphone = hphone
        self.cphone = cphone
        self.mophone = mophone
        self.faphone = faphone
        self.susp_phone = susp_phone
        self.email = email
        self.moemail = moemail
        self.faemail = faemail
        self.susp_email = susp_email
        self.ap_ceeb = ap_ceeb
        self.susp_ceeb = susp_ceeb
        self.ap_orgid = ap_orgid
        self.susp_orgid = susp_orgid
        self.rat = rat
        self.ape = ape
        self.adr = adr
        self.dob = dob
        self.nid = nid
        self.pho = pho
        self.ceeb = ceeb
        self.orgid = orgid
        self.pos = pos
        self.fn = fn
        self.em = em
        self.npl = npl
        self.ano = ano
        self.ln = ln
        
    def toArr(self):
        arr = [self.id1,self.id2,self.emplid,self.ano,self.ap_last,self.susp_last,self.ap_first,self.ap_pref,self.susp_first,self.ap_mid,self.smi,self.ax,self.sx,self.ap_dob,self.susp_dob,self.ap_nid,self.susp_nid,self.ap_ma1,self.ap_ha1,self.ap_sa1,self.susp_addr,self.ap_mcity,self.ap_hcity,self.ap_scity,self.susp_city,self.ap_mstate,self.ap_hstate,self.ap_sstate,self.susp_state,self.ap_mpostal,self.ap_hpostal,self.susp_postal,self.ap_mco,self.ap_hco,self.ap_sco,self.susp_co,self.hphone,self.cphone,self.mophone,self.faphone,self.susp_phone,self.email,self.moemail,self.faemail,self.susp_email,self.ap_ceeb,self.susp_ceeb,self.ap_orgid,self.susp_orgid,self.rat,self.ape,self.adr,self.dob,self.nid,self.pho,self.atp,self.pos,self.fn,self.em,self.npl,self.ln]
        return arr
