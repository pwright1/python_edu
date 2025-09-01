
# Copyright Aug 2025, Philip Wright. All rights reserved. 

from pw_utils import string_utils

class ActMatchResult:

    def __init__(self, sa, susp, mr):
        ahome = sa.homeBiodemo()
        amail = sa.mailBiodemo()
        bsusp = susp.biodemo()

        self.setVals(bsusp.id1,
                     bsusp.id2,
                     ahome.id2,  # emplid
                     susp.loaddate,
                     susp.actid,
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
                     f"{mr.maRating:02d}",
                     f"{mr.maApe():d}",
                     f"{mr.maAddr:d}",
                     f"{mr.maDob:d}",
                     f"{mr.maPhone:d}",
                     f"{mr.maCeeb:d}",
                     f"{mr.maPostal:d}",
                     f"{mr.maFirst:d}",
                     f"{mr.maEmail:d}",
                     sa.nplan,
                     sa.appno,
                     f"{mr.maLast:d}")
        pass # def init

    def header():
        hdr_str = """ACTRECORD_ID_ SCC_TEMP_ID_ EMPLID_ LDATE_ ACTID_ AP_LAST_ SUSP_LAST_ AP_FIRST_ AP_PREF_ SUSP_FIRST_ AP_MID_ SMI_ AX_ SX_ AP_DOB_ SUSP_DOB_ AP_MA1_ AP_HA1_ AP_SA1_ SUSP_ADDR_ AP_MCITY_ AP_HCITY_ AP_SCITY_ SUSP_CITY_ AP_MSTATE_ AP_HSTATE_ AP_SSTATE_ SUSP_STATE_ AP_MPOSTAL_ AP_HPOSTAL_ SUSP_POSTAL_ AP_MCO_ AP_HCO_ AP_SCO_ SUSP_CO_ HPHONE_ CPHONE_ MOPHONE_ FAPHONE_ SUSP_PHONE_ EMAIL_ MOEMAIL_ FAEMAIL_ SUSP_EMAIL_ AP_CEEB_ SUSP_CEEB_ RAT_ APE_ ADR_ DOB_ PHO_ CEEB_ POS_ FN_ EM_ NPL_ ANO_ LN_ FDUP_"""
        return string_utils.pct_w(hdr_str)
    
    def setVals(self, ahid,trec,emplid,ldate,actid,ap_last,susp_last,ap_first,ap_pref,susp_first,ap_mid,smi,ax,sx,ap_dob,susp_dob,ap_ma1,ap_ha1,ap_sa1,susp_addr,ap_mcity,ap_hcity,ap_scity,susp_city,ap_mstate,ap_hstate,ap_sstate,susp_state,ap_mpostal,ap_hpostal,susp_postal,ap_mco,ap_hco,ap_sco,susp_co,hphone,cphone,mophone,faphone,susp_phone,email,moemail,faemail,susp_email,ap_ceeb,susp_ceeb,rat,ape,adr,dob,pho,ceeb,pos,fn,em,npl,ano,ln):

        self.ahid = ahid
        self.trec = trec
        self.emplid = emplid
        self.ldate = ldate
        self.actid = actid
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
        self.rat = rat
        self.ape = ape
        self.adr = adr
        self.dob = dob
        self.pho = pho
        self.ceeb = ceeb
        self.pos = pos
        self.fn = fn
        self.em = em
        self.npl = npl
        self.ano = ano
        self.ln = ln
        self.false_dup = 0  # not using field
        
    def toArr(self):
        arr = [self.ahid,self.trec,self.emplid,self.ldate,self.actid,self.ap_last,self.susp_last,self.ap_first,self.ap_pref,self.susp_first,self.ap_mid,self.smi,self.ax,self.sx,self.ap_dob,self.susp_dob,self.ap_ma1,self.ap_ha1,self.ap_sa1,self.susp_addr,self.ap_mcity,self.ap_hcity,self.ap_scity,self.susp_city,self.ap_mstate,self.ap_hstate,self.ap_sstate,self.susp_state,self.ap_mpostal,self.ap_hpostal,self.susp_postal,self.ap_mco,self.ap_hco,self.ap_sco,self.susp_co,self.hphone,self.cphone,self.mophone,self.faphone,self.susp_phone,self.email,self.moemail,self.faemail,self.susp_email,self.ap_ceeb,self.susp_ceeb,self.rat,self.ape,self.adr,self.dob,self.pho,self.ceeb,self.pos,self.fn,self.em,self.npl,self.ano,self.ln, self.false_dup]
        return arr
