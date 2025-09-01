
# Copyright Aug 2025, Philip Wright. All rights reserved. 

from pw_utils import act_utils
from pw_utils import mydb_utils

# sup_comp_type field added, only difference between this and act2020csv.py
class Act2025csv:
    debug = False
    
    def __init__(self):
        if self.debug:
            print("class {} init".format(type(self)) )
        pass # def init

    def process_row(self, conn, fname, line, line_count, actfile_id, ts, fields, for_fake_stage=False):
        data_src, rep_dte, id_act, coll_cde, f_name, m_initial, l_name, address1, address2, city, state_full, state, zip5, zip4, country_txt, country_iso, country_cde, fips, county_name, prov, postal, phone, ph_ctrycde, ph_type, email, dob, gender, ethn_hisplat, ethn_alaskanat, ethn_asian, ethn_africamer, ethn_pacisland, ethn_white, ethn_prefnorsp, ethn_fedvalues, hs_gradyr, hs_code, hs_name, hs_type, hs_gradelev, coll_choicnum, major, major_code, cip_code, major_certainty, voc, voc_code, voc_certainty, intmaj_fit, single_hightest, test_dte, test_loc, id_stateassign, eng, mth, rdg, sci, composite, stem, writing, ela, sum_scale, sup_eng_dt, sup_eng_loc, sup_eng_sc, sup_otheng, sup_eng_type, sup_mth_dt, sup_mth_loc, sup_mth_sc, sup_othmth, sup_mth_type, sup_rdg_dt, sup_rdg_loc, sup_rdg_sc, sup_othrdg, sup_rdg_type, sup_sci_dt, sup_sci_loc, sup_sci_sc, sup_othsci, sup_sci_type, sup_comp_type, sup_composite, sup_stem, sup_writ_dt, sup_writ_loc, sup_writ_sc, sup_writ_type, sup_ela, full_part, live_pln, highest_ed_exp, rotc, help_edplan, help_writing, help_reading, help_studysk, help_mathsk, want_indepst, want_honors, want_studyab, plan_instmus, plan_vocmus, plan_stugov, plan_pubs, plan_debate, plan_theater, plan_relorg, plan_racethorg, plan_vathl, plan_polorg, plan_radiotv, plan_fratsor, plan_servorg, exp_finaidapp, exp_work, exp_workhrs, fam_income, edlevel_mthr, edlevel_fthr, pref_dist, insttypepref, insttypepref_rnk, malefempref, malefempref_rnk, statepref1, statepref2, statepref_rnk, costpref, costpref_rnk, sizepref, sizepref_rnk, acadmajpref_rnk, otherfac_rnk, rel_int, hs_size, hs_classrnk, hs_gpa_range, hs_curriculum, hs_gpa_calc, hs_yrs_eng, hs_yrs_mth, hs_yrs_soc, hs_yrs_natsc, hs_yrs_span, hs_yrs_germ, hs_yrs_frch, hs_yrs_othlang, hons_eng, hons_mth, hons_soc, hons_natsc, hons_lang, activ_instmus, activ_vocmus, activ_stugov, activ_pubs, activ_debate, activ_theater, activ_relorg, activ_racethorg, activ_vathl, activ_polorg, activ_radiotv, activ_socialclb, activ_servorg, c_eng_9, c_eng_10, c_eng_11, c_eng_12, c_eng_oth, c_alg1, c_alg2, c_geom, c_trig, c_begcalc, c_advmth, c_cmpt, c_gensc, c_bio, c_chem, c_phys, c_ushist, c_wrldhist, c_othhist, c_amgov, c_econ, c_geog, c_psych, c_span, c_frch, c_germ, c_lang, c_art, c_mus, c_dram, g_eng_9, g_eng_10, g_eng_11, g_eng_12, g_eng_oth, g_alg1, g_alg2, g_geom, g_trig, g_begcalc, g_advmth, g_cmpt, g_gensc, g_bio, g_chem, g_phys, g_ushist, g_wrldhist, g_othhist, g_amgov, g_econ, g_geog, g_psych, g_span, g_frch, g_germ, g_lang, g_art, g_mus, g_dram, g_alleng, g_allmth, g_allsocsc, g_allnatsc, g_overall, intinv_scistd, intinv_scirnk, intinv_artstd, intinv_artrnk, intinv_socstd, intinv_socrnk, intinv_buscstd, intinv_buscrank, intinv_busopstd, intinv_busoprnk, intinv_techstd, intinv_techrnk, intinv_mapreg1, intinv_mapreg2, intinv_mapreg3, cs_grp1_rnk, cs_grp2_rnk, cs_grp3_rnk, cs_grp4_rnk, cs_grp5_rnk, sub_grp1_rnk, sub_grp2_rnk, sub_grp3_rnk, sub_grp4_rnk, sub_grp5_rnk, instrnk_engl, instrnk_math, instrnk_read, instrnk_sci, instrnk_stem, instrnk_ela, rnks_type, instrnk_writ, instrnk_enrcomp, c_readiness, e_earn_prod, e_poss_prod, e_pctc_prod, e_rdyrng_prod, e_earn_knlg, e_poss_knlg, e_pctc_knlg, e_rdyrng_knlg, e_earn_conv, e_poss_conv, e_pctc_conv, e_rdyrng_conv, m_earn_higher, m_poss_higher, m_pctc_higher, m_rdyrng_higher, m_earn_numq, m_poss_numq, m_pctc_numq, m_rdyrng_numq, m_earn_alg, m_poss_alg, m_pctc_alg, m_rdyrng_alg, m_earn_func, m_poss_func, m_pctc_func, m_rdyrng_func, m_earn_geom, m_poss_geom, m_pctc_geom, m_rdyrng_geom, m_earn_stat, m_poss_stat, m_pctc_stat, m_rdyrng_stat, m_earn_essen, m_poss_essen, m_pctc_essen, m_rdyrng_essen, m_earn_mod, m_poss_mod, m_pctc_mod, m_rdyrng_mod, r_earn_ideas, r_poss_ideas, r_pctc_ideas, r_rdyrng_ideas, r_earn_struc, r_poss_struc, r_pctc_struc, r_rdyrng_struc, r_earn_knlg, r_poss_knlg, r_pctc_knlg, r_rdyrng_knlg, r_und_comp_txt, s_earn_data, s_poss_data, s_pctc_data, s_rdyrng_data, s_earn_inv, s_poss_inv, s_pctc_inv, s_rdyrng_inv, s_earn_eval, s_poss_eval, s_pctc_eval, s_rdyrng_eval, w_dom_ideas, w_dom_supt, w_dom_org, w_dom_languse, usrnk_sub_engl, usrnk_sub_math, usrnk_sub_rdg, usrnk_sub_sci, usrnk_comp, usrnk_stem, usrnk_wri, usrnk_ela, prob_c_group1, prob_c_group2, prob_c_group3, prob_c_group4, prob_c_group5, prob_c_course1, prob_c_course2, prob_c_course3, prob_c_course4, prob_c_course5, prob_b_group1, prob_b_group2, prob_b_group3, prob_b_group4, prob_b_group5, prob_b_course1, prob_b_course2, prob_b_course3, prob_b_course4, prob_b_course5, eos_optin, id_local, hist_eng, hist_math, hist_socst, hist_natsc, hist_comp = fields

        enable_score_load = not for_fake_stage

        # blank out the ethnicity fields for no local storage
        ethn_hisplat = ""
        ethn_alaskanat = ""
        ethn_asian = ""
        ethn_africamer = ""
        ethn_pacisland = ""
        ethn_white = ""
        ethn_prefnorsp = ""
        ethn_fedvalues = ""
        
        mm  = rep_dte[0:2]
        dd  = rep_dte[2:4]
        yy = rep_dte[4:8]
        rdte = f"{yy}-{mm}-{dd}"

        
        #print(rdte)
        vdob = ""
        vdob_stage = ""
        # you are storing it as edob but the data file has dob
        if len(dob) == 8:
            vdob = dob[4:8] + '-' + dob[0:2] + '-' +  dob[2:4]
            vdob_stage = dob[0:2] + '/' + dob[2:4] + '/' + dob[4:8]
        vcountry = country_iso.upper()
        vstate = ""
        vpostal = ""
        if vcountry == "USA":
            vpostal = zip5[0:5].strip()
            vstate = state[0:2].strip()
            
        if vstate == "FN" or vstate == "CN":
            vstate = ""

        # for digest we want the smallest of max size of (act_spec, ps_query_field)
        vlast = l_name.upper()[0:30].strip()
        vfirst = f_name.upper()[0:30].strip()
        vmi = m_initial.upper()[0:1].strip()
        vstreet = address1.upper()[0:40].strip()
        vemail = email.upper()[0:50].strip()
        vcity = city.upper()[0:30].strip()
        
        reportyear = rdte
        test_dte_yr = test_dte[2:6]
        test_dte_mo = test_dte[0:2]
        etestdate = f"{test_dte_yr}{test_dte_mo}"
        etestdate_stage = f"{test_dte_mo}/01/{test_dte_yr}"
        vgender = ""

        if gender == "Male":
            vgender = "M"
        elif gender == "Female":
            vgender = "F"
        else:
            vgender = "U"
        
        vdate_loaded = ts[0:10]
        stage_date_loaded = mydb_utils.iso_to_date(vdate_loaded)

        vsup_eng_dt = act_utils.mmyyyy_swap(sup_eng_dt)
        vsup_mth_dt = act_utils.mmyyyy_swap(sup_mth_dt)
        vsup_rdg_dt = act_utils.mmyyyy_swap(sup_rdg_dt)
        vsup_sci_dt = act_utils.mmyyyy_swap(sup_sci_dt)

        veng, veng_dt =               act_utils.score_date_check(eng, etestdate)
        vsup_eng_sc, vsup_eng_dt =    act_utils.score_date_check(sup_eng_sc, vsup_eng_dt)
        vmth, vmth_dt =               act_utils.score_date_check(mth, etestdate)
        vsup_mth_sc, vsup_mth_dt =    act_utils.score_date_check(sup_mth_sc, vsup_mth_dt)
        vrdg, vrdg_dt =               act_utils.score_date_check(rdg, etestdate)
        vsup_rdg_sc, vsup_rdg_dt =    act_utils.score_date_check(sup_rdg_sc, vsup_rdg_dt)
        vsci, vsci_dt =               act_utils.score_date_check(sci, etestdate)
        vsup_sci_sc, vsup_sci_dt =    act_utils.score_date_check(sup_sci_sc, vsup_sci_dt)
        vcomposite, vcomposite_dt =   act_utils.score_date_check(composite, etestdate)

        vsup_eng_dt_stage = ""
        if len(sup_eng_dt) > 0:
            vsup_eng_dt_stage = sup_eng_dt[0:2] + "/01/" + sup_eng_dt[2:6]

        vsup_mth_dt_stage = ""
        if len(sup_mth_dt) > 0:
            vsup_mth_dt_stage = sup_mth_dt[0:2] + "/01/" + sup_mth_dt[2:6]

        vsup_rdg_dt_stage = ""
        if len(sup_rdg_dt) > 0:
            vsup_rdg_dt_stage = sup_rdg_dt[0:2] + "/01/" + sup_rdg_dt[2:6]

        vsup_sci_dt_stage = ""
        if len(sup_sci_dt) > 0:
            vsup_sci_dt_stage = sup_sci_dt[0:2] + "/01/" + sup_sci_dt[2:6]

            
        if enable_score_load:

            digparts, digest = act_utils.calc_digest(vdate_loaded, veng, veng_dt, vsup_eng_sc, vsup_eng_dt, vmth, vmth_dt, vsup_mth_sc, vsup_mth_dt, vrdg, vrdg_dt, vsup_rdg_sc, vsup_rdg_dt, vsci, vsci_dt, vsup_sci_sc, vsup_sci_dt, vcomposite, vcomposite_dt, sup_composite, vlast, vfirst, vmi, vdob, vemail, vstreet, vcity, vstate, vpostal)
        
            digest_nld = act_utils.calc_digest_nld(veng, veng_dt, vsup_eng_sc, vsup_eng_dt, vmth, vmth_dt, vsup_mth_sc, vsup_mth_dt, vrdg, vrdg_dt, vsup_rdg_sc, vsup_rdg_dt, vsci, vsci_dt, vsup_sci_sc, vsup_sci_dt, vcomposite, vcomposite_dt, sup_composite, vlast, vfirst, vmi, vdob, vemail, vstreet, vcity, vstate, vpostal)
        
            writing = act_utils.calc_writing(w_dom_ideas, w_dom_supt, w_dom_org, w_dom_languse)

            tup = (actfile_id, line_count, reportyear, vlast, vfirst, vmi, address1,vcountry, vgender, hs_gradelev, id_act, ph_type, phone, vcity, state, zip5, vdob, '','','','','',id_stateassign, hs_code, hs_gpa_calc,  hs_gradyr,  etestdate, test_loc, prov, postal, '',eng, mth, rdg, sci, composite, email, instrnk_engl, instrnk_math, instrnk_read, instrnk_sci, instrnk_enrcomp,'',writing, w_dom_ideas, w_dom_supt, w_dom_org, w_dom_languse, instrnk_writ, ela, instrnk_ela, stem, instrnk_stem, '', '', writing, sup_comp_type, digest, digparts, digest_nld)
            actrecord_id = self.act_insert(conn, self.actrecord_q(), tup)

            tup1 = (actrecord_id, data_src, rep_dte, id_act, coll_cde, f_name, m_initial, l_name, address1, address2, city, state_full, state, zip5, zip4, country_txt, country_iso, country_cde, prov, postal, phone, ph_ctrycde, email, vdob, gender, hs_code, hs_name, hs_gradelev, test_dte, test_loc, id_stateassign, eng, mth, rdg, sci, composite, stem, writing, ela, sup_eng_dt, sup_eng_loc, sup_eng_sc, sup_otheng, sup_eng_type, sup_mth_dt, sup_mth_loc, sup_mth_sc, sup_othmth, sup_mth_type, sup_rdg_dt, sup_rdg_loc, sup_rdg_sc, sup_othrdg, sup_rdg_type, sup_sci_dt, sup_sci_loc, sup_sci_sc, sup_othsci, sup_sci_type, sup_composite, sup_stem, sup_writ_dt, sup_writ_loc, sup_writ_sc, sup_writ_type, sup_ela, w_dom_ideas, w_dom_supt, w_dom_org, w_dom_languse)
            self.act_insert(conn, self.actrecord_addl_1_q(), tup1)

            tup2 = (actrecord_id, fips, county_name, ph_type, ethn_hisplat, ethn_alaskanat, ethn_asian, ethn_africamer, ethn_pacisland, ethn_white, ethn_prefnorsp, ethn_fedvalues, hs_gradyr, hs_type, coll_choicnum, major, major_code, cip_code, major_certainty, voc, voc_code, voc_certainty, intmaj_fit, single_hightest, sum_scale, full_part, live_pln, highest_ed_exp, rotc, help_edplan, help_writing, help_reading, help_studysk, help_mathsk, want_indepst, want_honors, want_studyab, plan_instmus, plan_vocmus, plan_stugov, plan_pubs, plan_debate, plan_theater, plan_relorg, plan_racethorg, plan_vathl, plan_polorg, plan_radiotv, plan_fratsor, plan_servorg, exp_finaidapp, exp_work, exp_workhrs, fam_income, edlevel_mthr, edlevel_fthr, pref_dist, insttypepref, insttypepref_rnk, malefempref, malefempref_rnk, statepref1, statepref2, statepref_rnk, costpref, costpref_rnk, sizepref, sizepref_rnk, acadmajpref_rnk, otherfac_rnk, rel_int, hs_size, hs_classrnk, hs_gpa_range, hs_curriculum, hs_gpa_calc, hs_yrs_eng, hs_yrs_mth, hs_yrs_soc, hs_yrs_natsc, hs_yrs_span, hs_yrs_germ)
            self.act_insert(conn, self.actrecord_addl_2_q(), tup2)

            tup3 = (actrecord_id, hs_yrs_frch, hs_yrs_othlang, hons_eng, hons_mth, hons_soc, hons_natsc, hons_lang, activ_instmus, activ_vocmus, activ_stugov, activ_pubs, activ_debate, activ_theater, activ_relorg, activ_racethorg, activ_vathl, activ_polorg, activ_radiotv, activ_socialclb, activ_servorg, c_eng_9, c_eng_10, c_eng_11, c_eng_12, c_eng_oth, c_alg1, c_alg2, c_geom, c_trig, c_begcalc, c_advmth, c_cmpt, c_gensc, c_bio, c_chem, c_phys, c_ushist, c_wrldhist, c_othhist, c_amgov, c_econ, c_geog, c_psych, c_span, c_frch, c_germ, c_lang, c_art, c_mus, c_dram, g_eng_9, g_eng_10, g_eng_11, g_eng_12, g_eng_oth, g_alg1, g_alg2, g_geom, g_trig, g_begcalc, g_advmth, g_cmpt, g_gensc, g_bio, g_chem, g_phys, g_ushist, g_wrldhist, g_othhist, g_amgov, g_econ, g_geog, g_psych, g_span, g_frch, g_germ, g_lang, g_art, g_mus, g_dram)
            self.act_insert(conn, self.actrecord_addl_3_q(), tup3)

            tup4 = (actrecord_id, g_alleng, g_allmth, g_allsocsc, g_allnatsc, g_overall, intinv_scistd, intinv_scirnk, intinv_artstd, intinv_artrnk, intinv_socstd, intinv_socrnk, intinv_buscstd, intinv_buscrank, intinv_busopstd, intinv_busoprnk, intinv_techstd, intinv_techrnk, intinv_mapreg1, intinv_mapreg2, intinv_mapreg3, cs_grp1_rnk, cs_grp2_rnk, cs_grp3_rnk, cs_grp4_rnk, cs_grp5_rnk, sub_grp1_rnk, sub_grp2_rnk, sub_grp3_rnk, sub_grp4_rnk, sub_grp5_rnk, instrnk_engl, instrnk_math, instrnk_read, instrnk_sci, instrnk_stem, instrnk_ela, rnks_type, instrnk_writ, instrnk_enrcomp, c_readiness, e_earn_prod, e_poss_prod, e_pctc_prod, e_rdyrng_prod, e_earn_knlg, e_poss_knlg, e_pctc_knlg, e_rdyrng_knlg, e_earn_conv, e_poss_conv, e_pctc_conv, e_rdyrng_conv, m_earn_higher, m_poss_higher, m_pctc_higher, m_rdyrng_higher, m_earn_numq, m_poss_numq, m_pctc_numq, m_rdyrng_numq, m_earn_alg, m_poss_alg, m_pctc_alg, m_rdyrng_alg, m_earn_func, m_poss_func, m_pctc_func, m_rdyrng_func, m_earn_geom, m_poss_geom, m_pctc_geom, m_rdyrng_geom, m_earn_stat, m_poss_stat, m_pctc_stat, m_rdyrng_stat, m_earn_essen, m_poss_essen, m_pctc_essen, m_rdyrng_essen)
            self.act_insert(conn, self.actrecord_addl_4_q(), tup4)
        
            tup5 = (actrecord_id, m_earn_mod, m_poss_mod, m_pctc_mod, m_rdyrng_mod, r_earn_ideas, r_poss_ideas, r_pctc_ideas, r_rdyrng_ideas, r_earn_struc, r_poss_struc, r_pctc_struc, r_rdyrng_struc, r_earn_knlg, r_poss_knlg, r_pctc_knlg, r_rdyrng_knlg, r_und_comp_txt, s_earn_data, s_poss_data, s_pctc_data, s_rdyrng_data, s_earn_inv, s_poss_inv, s_pctc_inv, s_rdyrng_inv, s_earn_eval, s_poss_eval, s_pctc_eval, s_rdyrng_eval, usrnk_sub_engl, usrnk_sub_math, usrnk_sub_rdg, usrnk_sub_sci, usrnk_comp, usrnk_stem, usrnk_wri, usrnk_ela, prob_c_group1, prob_c_group2, prob_c_group3, prob_c_group4, prob_c_group5, prob_c_course1, prob_c_course2, prob_c_course3, prob_c_course4, prob_c_course5, prob_b_group1, prob_b_group2, prob_b_group3, prob_b_group4, prob_b_group5, prob_b_course1, prob_b_course2, prob_b_course3, prob_b_course4, prob_b_course5, eos_optin, id_local, hist_eng, hist_math, hist_socst, hist_natsc, hist_comp)
            self.act_insert(conn, self.actrecord_addl_5_q(), tup5)
        
        # for fake stage = true 
        else:
            seq = self.act_insert(conn, self.act_stage_fake_seq_q(), ("test_value",))

            if veng != "" and veng_dt != "":
                tup6 = (seq, "ACT","ENGL",etestdate_stage,"ACT",veng, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, vcountry, vdob_stage, phone, "","","LD","","")
                self.act_insert(conn, self.act_stage_fake_q(),tup6)

            if vmth != "" and vmth_dt != "":
                tup6 = (seq, "ACT","MATH",etestdate_stage,"ACT",vmth, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, vcountry, vdob_stage, phone, "","","LD","","")
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vrdg != "" and vrdg_dt != "":
                tup6 = (seq, "ACT","READ",etestdate_stage,"ACT",vrdg, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, vcountry, vdob_stage, phone, "","","LD","","")
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vsci != "" and vsci_dt != "":
                tup6 = (seq, "ACT","SCIRE",etestdate_stage,"ACT",vsci, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, vcountry, vdob_stage, phone, "","","LD","","")
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vcomposite != "" and vcomposite_dt != "":
                tup6 = (seq, "ACT","COMP",etestdate_stage,"ACT",vcomposite, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, vcountry, vdob_stage, phone, "","","LD","","")
                self.act_insert(conn, self.act_stage_fake_q(),tup6)

            if vsup_eng_sc != "" and vsup_eng_dt != "":
                tup6 = (seq, "ACT","ENGLS",vsup_eng_dt_stage,"ACT",vsup_eng_sc, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, vcountry, vdob_stage, phone, "","","LD","","")
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vsup_mth_sc != "" and vsup_mth_dt != "":
                tup6 = (seq, "ACT","MATHS",vsup_mth_dt_stage,"ACT",vsup_mth_sc, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, vcountry, vdob_stage, phone, "","","LD","","")
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vsup_rdg_sc != "" and vsup_rdg_dt != "":
                tup6 = (seq, "ACT","READS",vsup_rdg_dt_stage,"ACT",vsup_rdg_sc, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, vcountry, vdob_stage, phone, "","","LD","","")
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
            if vsup_sci_sc != "" and vsup_sci_dt != "":
                tup6 = (seq, "ACT","SCISS",vsup_sci_dt_stage,"ACT",vsup_sci_sc, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, vcountry, vdob_stage, phone, "","","LD","","")
                self.act_insert(conn, self.act_stage_fake_q(),tup6)

            if sup_composite != "":
                tup6 = (seq, "ACT","COMPS",etestdate_stage,"ACT",sup_composite, stage_date_loaded, vlast, vfirst, vmi, vemail, vstreet, vcity,vstate, vpostal, country, vdob_stage, phone, "","","LD","","",actrecord_id)
                self.act_insert(conn, self.act_stage_fake_q(),tup6)
            
        #tup = (actrecord_id, id_act, vlast, vfirst, vmi, vgender, vdob, address1, vcity, vstate, vpostal, vcountry, vemail, phone, hs_code, etestdate, veng, vmth, vrdg, vsci, vcomposite, id_stateassign, vdate_loaded, digparts, digest, digest_nld)
        #self.act_insert(conn, self.act_local_susp_q(), tup)
        
    def act_insert(self, conn, q, tup):
        cur = conn.cursor()
        cur.execute(q, tup)
        lastrowid = cur.lastrowid
        return lastrowid
        
    def actrecord_q(self):
        q = """
        insert into actrecord(actfile_id, fileline, reportyear, last, first, mi, street, country_code, genderalpha, gradelevel, actid, phonetype, phone, city, state, zip5, edob, ewcomb, wrsub, ewcombnorm, wrsubnorm, wrdescr, stateid, hscode, hsavg, gradyr, etestdate, testloc, cnprovince, cnpostal, corrrpt, engl, math, read, scire, comp, email, engl_norm, math_norm, read_norm, scire_norm, comp_norm, writ_subj, writ_subj16, writ_dom1, writ_dom2, writ_dom3, writ_dom4, writ_norm, ela_score, ela_norm, stem_score, stem_norm, ucomplex_ind, prog_ind, writ_dom_avg, sup_comp_type, digest, digparts, digest_nld) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        return q

    def actrecord_addl_1_q(self):
        q = """
          insert into actrecord_addl_1(actrecord_id, data_src, rep_dte, id_act, coll_cde, f_name, m_initial, l_name, address1, address2, city, state_full, state, zip5, zip4, country_txt, country_iso, country_cde, prov, postal, phone, ph_ctrycde, email, dob, gender, hs_code, hs_name, hs_gradelev, test_dte, test_loc, id_stateassign, eng, mth, rdg, sci, composite, stem, writing, ela, sup_eng_dt, sup_eng_loc, sup_eng_sc, sup_otheng, sup_eng_type, sup_mth_dt, sup_mth_loc, sup_mth_sc, sup_othmth, sup_mth_type, sup_rdg_dt, sup_rdg_loc, sup_rdg_sc, sup_othrdg, sup_rdg_type, sup_sci_dt, sup_sci_loc, sup_sci_sc, sup_othsci, sup_sci_type, sup_composite, sup_stem, sup_writ_dt, sup_writ_loc, sup_writ_sc, sup_writ_type, sup_ela, w_dom_ideas, w_dom_supt, w_dom_org, w_dom_languse) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        return q

    def actrecord_addl_2_q(self):
        q = """
          insert into actrecord_addl_2(actrecord_id, fips, county_name, ph_type, ethn_hisplat, ethn_alaskanat, ethn_asian, ethn_africamer, ethn_pacisland, ethn_white, ethn_prefnorsp, ethn_fedvalues, hs_gradyr, hs_type, coll_choicnum, major, major_code, cip_code, major_certainty, voc, voc_code, voc_certainty, intmaj_fit, single_hightest, sum_scale, full_part, live_pln, highest_ed_exp, rotc, help_edplan, help_writing, help_reading, help_studysk, help_mathsk, want_indepst, want_honors, want_studyab, plan_instmus, plan_vocmus, plan_stugov, plan_pubs, plan_debate, plan_theater, plan_relorg, plan_racethorg, plan_vathl, plan_polorg, plan_radiotv, plan_fratsor, plan_servorg, exp_finaidapp, exp_work, exp_workhrs, fam_income, edlevel_mthr, edlevel_fthr, pref_dist, insttypepref, insttypepref_rnk, malefempref, malefempref_rnk, statepref1, statepref2, statepref_rnk, costpref, costpref_rnk, sizepref, sizepref_rnk, acadmajpref_rnk, otherfac_rnk, rel_int, hs_size, hs_classrnk, hs_gpa_range, hs_curriculum, hs_gpa_calc, hs_yrs_eng, hs_yrs_mth, hs_yrs_soc, hs_yrs_natsc, hs_yrs_span, hs_yrs_germ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        return q

    def actrecord_addl_3_q(self):
        q = """
        insert into actrecord_addl_3(actrecord_id, hs_yrs_frch, hs_yrs_othlang, hons_eng, hons_mth, hons_soc, hons_natsc, hons_lang, activ_instmus, activ_vocmus, activ_stugov, activ_pubs, activ_debate, activ_theater, activ_relorg, activ_racethorg, activ_vathl, activ_polorg, activ_radiotv, activ_socialclb, activ_servorg, c_eng_9, c_eng_10, c_eng_11, c_eng_12, c_eng_oth, c_alg1, c_alg2, c_geom, c_trig, c_begcalc, c_advmth, c_cmpt, c_gensc, c_bio, c_chem, c_phys, c_ushist, c_wrldhist, c_othhist, c_amgov, c_econ, c_geog, c_psych, c_span, c_frch, c_germ, c_lang, c_art, c_mus, c_dram, g_eng_9, g_eng_10, g_eng_11, g_eng_12, g_eng_oth, g_alg1, g_alg2, g_geom, g_trig, g_begcalc, g_advmth, g_cmpt, g_gensc, g_bio, g_chem, g_phys, g_ushist, g_wrldhist, g_othhist, g_amgov, g_econ, g_geog, g_psych, g_span, g_frch, g_germ, g_lang, g_art, g_mus, g_dram) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        return q

    def actrecord_addl_4_q(self):
        q = """
        insert into actrecord_addl_4(actrecord_id, g_alleng, g_allmth, g_allsocsc, g_allnatsc, g_overall, intinv_scistd, intinv_scirnk, intinv_artstd, intinv_artrnk, intinv_socstd, intinv_socrnk, intinv_buscstd, intinv_buscrank, intinv_busopstd, intinv_busoprnk, intinv_techstd, intinv_techrnk, intinv_mapreg1, intinv_mapreg2, intinv_mapreg3, cs_grp1_rnk, cs_grp2_rnk, cs_grp3_rnk, cs_grp4_rnk, cs_grp5_rnk, sub_grp1_rnk, sub_grp2_rnk, sub_grp3_rnk, sub_grp4_rnk, sub_grp5_rnk, instrnk_engl, instrnk_math, instrnk_read, instrnk_sci, instrnk_stem, instrnk_ela, rnks_type, instrnk_writ, instrnk_enrcomp, c_readiness, e_earn_prod, e_poss_prod, e_pctc_prod, e_rdyrng_prod, e_earn_knlg, e_poss_knlg, e_pctc_knlg, e_rdyrng_knlg, e_earn_conv, e_poss_conv, e_pctc_conv, e_rdyrng_conv, m_earn_higher, m_poss_higher, m_pctc_higher, m_rdyrng_higher, m_earn_numq, m_poss_numq, m_pctc_numq, m_rdyrng_numq, m_earn_alg, m_poss_alg, m_pctc_alg, m_rdyrng_alg, m_earn_func, m_poss_func, m_pctc_func, m_rdyrng_func, m_earn_geom, m_poss_geom, m_pctc_geom, m_rdyrng_geom, m_earn_stat, m_poss_stat, m_pctc_stat, m_rdyrng_stat, m_earn_essen, m_poss_essen, m_pctc_essen, m_rdyrng_essen) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        return q

    def actrecord_addl_5_q(self):
        q = """
        insert into actrecord_addl_5(actrecord_id, m_earn_mod, m_poss_mod, m_pctc_mod, m_rdyrng_mod, r_earn_ideas, r_poss_ideas, r_pctc_ideas, r_rdyrng_ideas, r_earn_struc, r_poss_struc, r_pctc_struc, r_rdyrng_struc, r_earn_knlg, r_poss_knlg, r_pctc_knlg, r_rdyrng_knlg, r_und_comp_txt, s_earn_data, s_poss_data, s_pctc_data, s_rdyrng_data, s_earn_inv, s_poss_inv, s_pctc_inv, s_rdyrng_inv, s_earn_eval, s_poss_eval, s_pctc_eval, s_rdyrng_eval, usrnk_sub_engl, usrnk_sub_math, usrnk_sub_rdg, usrnk_sub_sci, usrnk_comp, usrnk_stem, usrnk_wri, usrnk_ela, prob_c_group1, prob_c_group2, prob_c_group3, prob_c_group4, prob_c_group5, prob_c_course1, prob_c_course2, prob_c_course3, prob_c_course4, prob_c_course5, prob_b_group1, prob_b_group2, prob_b_group3, prob_b_group4, prob_b_group5, prob_b_course1, prob_b_course2, prob_b_course3, prob_b_course4, prob_b_course5, eos_optin, id_local, hist_eng, hist_math, hist_socst, hist_natsc, hist_comp) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        return q

    #def act_local_susp_q(self):
    #    q = """
    #    insert into act_local_susp(actrecord_id, actid, last, first, mi, gender, dob, street, city, state, postal, country, email, phone, hscode, etestdate, engl, math, read, scire, comp, stateid, loaddate, digparts, digest, digest_nld) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    #    """
    #    return q

    def act_stage_fake_q(self):
        q = """
        insert into act_stage_fake(siss_id, test_id, test_component, test_date, data_src, score, date_loaded, last, first, middle, email, address1, city, state, postal, country, birthdate, homephone, cphone, ophone, status, created, last_update) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        return q
    def act_stage_fake_seq_q(self):
        q = """
        insert into act_stage_fake_seq (test_value) values (?)
        """
        return q
