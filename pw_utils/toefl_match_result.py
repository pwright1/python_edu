
# Copyright Sep 2025, Philip Wright. All rights reserved. 

from pw_utils import string_utils

class ToeflScore:
    def __init__(self, admin_date, test_type, ibt_listening, ibt_reading, ibt_speaking, ibt_writing, ibt_total, pb_sec1, pb_sec2, pb_sec3, pb_conv_twe, pb_total, rpdt_listening, rpdt_reading, rpdt_writing, ran, filename, fileline):

        self.tdate = admin_date
        self.ttype = test_type
        self.ibt_list = ibt_listening
        self.ibt_read = ibt_reading
        self.ibt_spea = ibt_speaking
        self.ibt_writ = ibt_writing
        self.ibt_tot = ibt_total
        self.pb_sec1 = pb_sec1
        self.pb_sec2 = pb_sec2
        self.pb_sec3 = pb_sec3
        self.pb_conv_twe = pb_conv_twe
        self.pb_total = pb_total
        self.rpdt_list = rpdt_listening
        self.rpdt_read = rpdt_reading
        self.rpdt_writ = rpdt_writing
        self.ran = ran
        self.filename = filename
        self.line = fileline

    def toArr(self):
        return [self.tdate,self.ttype,self.ibt_list,self.ibt_read,self.ibt_spea,self.ibt_writ,self.ibt_tot,self.pb_sec1,self.pb_sec2,self.pb_sec3,self.pb_conv_twe,self.pb_total,self.rpdt_list,self.rpdt_read,self.rpdt_writ,self.ran,self.filename,self.line]


class ToeflMatchResult:

    def header():
        hdr = string_utils.pct_w("TOEFL_RECORD_ID__ SCC_TEMP_ID_ EMPLID_ AP_LAST_ SUSP_LAST_ AP_FIRST_ AP_PREF_ SUSP_FIRST_ AP_MID_ SUSP_MID_ AX_ SX_ AP_DOB_ SUSP_DOB_ AP_MA1_ AP_HA1_ AP_SA1_ SU_A1_ SU_A2_ SU_A3_ SU_A4_ AP_MCITY_ AP_HCITY_ AP_SCITY_ SUSP_CITY_ AP_MSTATE_ AP_HSTATE_ AP_SSTATE_ SUSP_STATE_ AP_MPOSTAL_ AP_HPOSTAL_ SUSP_POSTAL_ AP_MCO_ AP_HCO_ AP_SCO_ SUSP_CO_ EMAIL_ MOEMAIL_ FAEMAIL_ SUSP_EMAIL_ RAT_ AE_ ADR_ DOB_ POS_ FN_ EM_ PEM_ NPL_ ANO_ LN_ TDATE_ TTYPE_ IBT_LIST_ IBT_READ_ IBT_SPEA_ IBT_WRIT_ IBT_TOT_ PB_SEC1_ PB_SEC2_ PB_SEC3_ PB_CONV_TWE_ PB_TOTAL_ RPDT_LIST_ RPDF_READ_ RPDT_WRIT_ RAN_ FILE_ LINE_")
        return hdr
        
    def __init__(self, sa, susp, mr, toefl_score):
        ahome = sa.homeBiodemo()
        amail = sa.mailBiodemo()
        bsusp = susp.biodemo()
        self.tscore = toefl_score
        
        
        self.set_vals(bsusp.id1,
                      bsusp.id2,
                      #ahome.id1, #slate id
                      ahome.id2, # emplid
                      ahome.last,
                      bsusp.last,
                      ahome.first,
                      ahome.pref,
                      bsusp.first,
                      ahome.middle,
                      bsusp.middle,
                      ahome.sex,
                      bsusp.sex,
                      ahome.dob,
                      bsusp.dob,
                      amail.addr,
                      ahome.addr,
                      ahome.saddr,
                      bsusp.addr,
                      bsusp.addr2,
                      bsusp.addr3,
                      bsusp.addr4,
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
                      ahome.email,
                      ahome.email2,
                      ahome.email3,
                      bsusp.email,
                      f"{mr.maRating:02d}",
                      f"{mr.maApe():d}",
                      f"{mr.maAddr:d}",
                      f"{mr.maDob:d}",
                      f"{mr.maPostal:d}",
                      f"{mr.maFirst:d}",
                      f"{mr.maEmail:d}",
                      f"{mr.maPEmail:d}",
                      sa.nplan,
                      sa.appno,
                      f"{mr.maLast:d}")
                     
    def set_vals(self, ahid,trec,emplid,ap_last,susp_last,ap_first,ap_pref,susp_first,ap_mid,susp_middle,ax,sx,ap_dob,susp_dob,ap_ma1,ap_ha1,ap_sa1,susp_addr1,susp_addr2,susp_addr3,susp_addr4,ap_mcity,ap_hcity,ap_scity,susp_city,ap_mstate,ap_hstate,ap_sstate,susp_state,ap_mpostal,ap_hpostal,susp_postal,ap_mco,ap_hco,ap_sco,susp_co,email,moemail,faemail,susp_email,rat,ape,adr,dob,pos,fn,em,pem,npl,ano,ln):

        self.ahid = ahid
        self.trec = trec
        self.emplid = emplid
        self.ap_last = ap_last.upper()
        self.susp_last = susp_last
        self.ap_first = ap_first.upper()
        self.ap_pref = ap_pref.upper()
        self.susp_first = susp_first
        self.ap_mid = ap_mid.upper()
        self.susp_middle = susp_middle
        self.ax = ax
        self.sx = sx
        self.ap_dob = ap_dob
        self.susp_dob = susp_dob
        self.ap_ma1 = ap_ma1
        self.ap_ha1 = ap_ha1
        self.ap_sa1 = ap_sa1
        self.susp_addr1 = susp_addr1
        self.susp_addr2 = susp_addr2
        self.susp_addr3 = susp_addr3
        self.susp_addr4 = susp_addr4
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
        self.email = email
        self.moemail = moemail
        self.faemail = faemail
        self.susp_email = susp_email
        self.rat = rat
        self.ape = ape
        self.adr = adr
        self.dob = dob
        self.pos = pos
        self.fn = fn
        self.em = em
        self.pem = pem
        self.npl = npl
        self.ano = ano
        self.ln = ln
        
        
    def toArr(self):
        arr = [self.ahid, self.trec, self.emplid, self.ap_last, self.susp_last, self.ap_first, self.ap_pref, self.susp_first, self.ap_mid, self.susp_middle, self.ax, self.sx, self.ap_dob, self.susp_dob, self.ap_ma1, self.ap_ha1, self.ap_sa1, self.susp_addr1, self.susp_addr2, self.susp_addr3, self.susp_addr4, self.ap_mcity, self.ap_hcity, self.ap_scity, self.susp_city, self.ap_mstate, self.ap_hstate, self.ap_sstate, self.susp_state, self.ap_mpostal, self.ap_hpostal, self.susp_postal, self.ap_mco, self.ap_hco, self.ap_sco, self.susp_co, self.email, self.moemail, self.faemail, self.susp_email, self.rat, self.ape, self.adr, self.dob, self.pos, self.fn, self.em, self.pem, self.npl, self.ano, self.ln]

        score = self.tscore.toArr()
        return arr + score

