
# Copyright Aug 2025, Philip Wright. All rights reserved. 

import hashlib
from pw_utils import act_utils
from pw_utils import act2020csv

class Act2016txt:
    debug = False
    
    def __init__(self):
        if self.debug:
            print("class {} init".format(type(self)) )
        pass # def init

    def process_row(self, conn, fname, line, line_count, actfile_id, ts):
        #print(f"Act2016txt process row {fname} {line_count}")
        
        reportyear     = line[0:2]
        last           = line[2:27].strip()
        first          = line[27:43].strip()
        mi             = line[43:44].strip()
        street         = line[44:84].strip()
        country_code   = line[84:86].strip()
        genderalpha    = line[87:88].strip()
        gradelevel     = line[88:90].strip()
        actid          = line[90:99].strip() 
        phonetype      = line[99:100].strip()
        phone          = line[106:116].strip()
        city           = line[116:141].strip()
        state          = line[143:145].strip()
        zip5           = line[145:154].strip()
        edob           = line[154:162].strip()
        ewcomb         = line[162:164].strip()
        wrsub          = line[164:166].strip()
        ewcombnorm     = line[166:168].strip()
        wrsubnorm      = line[168:170].strip()
        wrdescr        = line[170:178].strip()
        stateid        = line[191:204].strip()
        hscode         = line[204:210].strip()
        hsavg          = line[219:222].strip()
        gradyr         = line[222:226].strip()
        etestdate      = line[226:232].strip()
        testloc        = line[248:249].strip()
        cnprovince     = line[249:251].strip()
        cnpostal       = line[252:259].strip()
        corrrpt        = line[259:260].strip()
        engl           = line[260:262].strip()
        math           = line[262:264].strip()
        read           = line[264:266].strip()
        scire          = line[266:268].strip()
        comp           = line[268:270].strip()
        dukeact         = line[315:319].strip()
        #if dukeact != "3088":
        #    raise RuntimeError(f"dukeact not 3088 but {dukeact}")
        email          = line[550:600].strip()
        engl_norm      = line[772:774].strip()
        math_norm      = line[774:776].strip()
        read_norm      = line[776:778].strip()
        scire_norm     = line[778:780].strip()
        comp_norm      = line[780:782].strip()
        #writ_subj      = line[790:792].strip()   #was PS WRS
        writ_subj = ""
        writ_dom1      = line[795:797].strip()   #was PS WDIA
        writ_dom2      = line[797:799].strip()   #was PS WDDS
        writ_dom3      = line[799:801].strip()   #was PS WDDO
        writ_dom4      = line[801:803].strip()   #was PS WDLC
        #writ_norm      = line[803:805].strip()
        writ_norm      = line[792:795].strip()
        writ_subj16    = line[803:805].strip()   # PS WRS16  02-12 tests >= 2016-09
        ela_score      = line[805:807].strip() 
        ela_norm       = line[807:809].strip()
        stem_score     = line[812:814].strip()
        stem_norm      = line[814:816].strip()
        ucomplex_ind   = line[819:820].strip()
        prog_ind       = line[820:821].strip()
        writ_dom_avg = act_utils.calc_writing(writ_dom1, writ_dom2, writ_dom3, writ_dom4)

        vdob = ""
        if len(edob) == 8:
            vdob = edob[0:4] + '-' + edob[4:6] + '-' +  edob[6:8]
        
        vlast = last.upper()[0:30].strip()
        vfirst = first.upper()[0:30].strip()
        vmi = mi.upper()[0:1].strip()
        vstreet = street.upper()[0:40].strip()
        vemail = email.upper()[0:50].strip()
        vcity = city.upper()[0:30].strip()
        vstate = state.strip()
        vpostal = zip5[0:5].strip()

        if country_code == "US":
            vstate = state
            vpostal = zip5[0:5]
        else:
            vstate = ""
            vpostal = ""
            
        vdate_loaded = ts[0:10]

        vengl, vengl_dt =             act_utils.score_date_check(engl, etestdate)
        vmath, vmath_dt =             act_utils.score_date_check(math, etestdate)
        vread, vread_dt =             act_utils.score_date_check(read, etestdate)
        vscire, vscire_dt =           act_utils.score_date_check(scire, etestdate)
        vcomp, vcomp_dt =             act_utils.score_date_check(comp, etestdate)

        if vstate == "FN" or vstate == "CN":
            vstate = ""
        
        digparts, digest = act_utils.calc_digest(vdate_loaded, vengl,vengl_dt, "", "", vmath,vmath_dt, "", "",vread,vread_dt, "", "",vscire,vscire_dt, "", "",vcomp,vcomp_dt, "", vlast,vfirst,vmi,vdob,vemail,vstreet,vcity,vstate,vpostal)

        digest_nld = act_utils.calc_digest_nld(vengl,vengl_dt, "", "", vmath,vmath_dt, "", "",vread,vread_dt, "", "",vscire,vscire_dt, "", "",vcomp,vcomp_dt, "", vlast,vfirst,vmi,vdob,vemail,vstreet,vcity,vstate,vpostal)

        tup = (actfile_id, line_count, reportyear, last, first, mi, street, country_code, genderalpha, gradelevel, actid, phonetype, phone, city, state, zip5, vdob, ewcomb, wrsub, ewcombnorm, wrsubnorm, wrdescr, stateid, hscode, hsavg, gradyr, etestdate, testloc, cnprovince, cnpostal, corrrpt, engl, math, read, scire, comp, email, engl_norm, math_norm, read_norm, scire_norm, comp_norm, writ_subj, writ_subj16, writ_dom1, writ_dom2, writ_dom3, writ_dom4, writ_norm, ela_score, ela_norm, stem_score, stem_norm, ucomplex_ind, prog_ind, writ_dom_avg, digest, digparts)

        actrecord_id = self.act_insert(conn, self.actrecord_q(), tup)

        #tup = (actrecord_id, actid, vlast, vfirst, vmi, genderalpha, vdob, street, vcity, vstate, vpostal, country_code, vemail, phone, hscode, etestdate, vengl, vmath, vread, vscire, vcomp, stateid, vdate_loaded, digparts, digest, digest_nld)
        #self.act_insert(conn, self.act_local_susp_q(), tup)

        
    def act_insert(self, conn, q, tup):
        cur = conn.cursor()
        cur.execute(q, tup)
        lastrowid = cur.lastrowid
        return lastrowid
        
    def actrecord_q(self):
        q = """
        insert into actrecord(actfile_id, fileline, reportyear, last, first, mi, street, country_code, genderalpha, gradelevel, actid, phonetype, phone, city, state, zip5, edob, ewcomb, wrsub, ewcombnorm, wrsubnorm, wrdescr, stateid, hscode, hsavg, gradyr, etestdate, testloc, cnprovince, cnpostal, corrrpt, engl, math, read, scire, comp, email, engl_norm, math_norm, read_norm, scire_norm, comp_norm, writ_subj, writ_subj16, writ_dom1, writ_dom2, writ_dom3, writ_dom4, writ_norm, ela_score, ela_norm, stem_score, stem_norm, ucomplex_ind, prog_ind, writ_dom_avg, digest, digparts) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        return q
        
#    def act_local_susp_q(self):
#        q = """
#        insert into act_local_susp(actrecord_id, actid, last, first, mi, gender, dob, street, city, state, postal, country, email, phone, hscode, etestdate, engl, math, read, scire, comp, stateid, loaddate, digparts, digest, digest_nld) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
#        """
#        return q
