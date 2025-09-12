# coding: utf-8
# -*- coding: utf-8 -*-

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sqlite3

table_notes = """
need to add index creation functions
DONT FORGET PRAGMA foreign keys in dotfile  / login
"""

def applicant_biod_table_delete(conn):
    delete_q = "delete from applicant_biod"
    cur = conn.cursor()
    cur.execute(delete_q)
    conn.commit()
    return

def applicant_biod_table_drop(conn):
    delete_q = "drop table if exists applicant_biod"
    cur = conn.cursor()
    cur.execute(delete_q)
    conn.commit()
    return

def applicant_biod_table_create(conn):
    q = """
    create table if not exists applicant_biod (
    SLATEID           text,
    EMPLID            text,
    APPNO             text,
    NPLAN             text,
    ADMITTYPE         text,
    ADMITTERM         text,
    FIRST             text,
    MIDDLE            text,
    LAST              text,
    PREFNAME          text,
    SUFFIX            text,
    SEX               text,
    BIRTHDATE         text,
    EMAIL             text,
    FATHEREMAIL       text,
    MOTHEREMAIL       text,
    CELLPHONE         text,
    HOMEPHONE         text,
    FATHERPHONE       text,
    MOTHERPHONE       text,
    MADDRESS1         text,
    MADDRESS2         text,
    MADDRESS3         text,
    MADDRESS4         text,
    MCITY             text,
    MSTATE            text,
    MPOSTAL           text,
    MCOUNTRY          text,
    MCOUNTRYDE        text,
    HADDRESS1         text,
    HADDRESS2         text,
    HADDRESS3         text,
    HADDRESS4         text,
    HCITY             text,
    HSTATE            text,
    HPOSTAL           text,
    HCOUNTRY          text,
    HCOUNTRYDE        text,
    SNAME             text,
    SADDRESS1         text,
    SADDRESS2         text,
    SCITY             text,
    SSTATE            text,
    SPOSTAL           text,
    SCOUNTRY          text,
    ORGID             text,
    CEEB              text,
    TEST_CONSIDER     text default '',
    primary key (emplid, appno))

    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actstage_table_drop(conn):
    q = "drop table if exists actstage"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actstage_table_create(conn):

    actstage_create_q = """
    create table if not exists actstage(
    scc_temp_id       integer,
    test_id           text,
    ls_data_source    text,
    date_loaded       text,
    last_name         text,
    first_name        text,
    middle_name       text,
    email_addr        text,
    address1          text,
    city              text,
    state             text,
    postal            text,
    country           text,
    birthdate         text,
    hphone            text,
    cphone            text,
    ophone            text,
    status            text,
    created           text,
    lastupd           text,
    emplid            text,
    engl              text,
    engl_dt           text,
    engls             text,
    engls_dt          text,
    math              text,
    math_dt           text,
    maths             text,
    maths_dt          text,
    read              text,
    read_dt           text,
    reads             text,
    reads_dt          text,
    scire             text,
    scire_dt          text,
    sciss             text,
    sciss_dt          text,
    comp              text,
    comp_dt           text,
    comps             text,
    stem              text,
    stem_dt           text,
    stems             text,
    stems_dt          text,
    ela               text,
    ela_dt            text,
    elass             text,
    elass_dt          text,
    wrs               text,
    wrs_dt            text,
    wrs16             text,
    wrs16_dt          text,
    wrsss             text,
    wrsss_dt          text,
    wdia              text,
    wdia_dt           text,
    wdds              text,
    wdds_dt           text,
    wdo               text,
    wdo_dt            text,
    wdlc              text,
    wdlc_dt           text,
    ew                text,
    ew_dt             text,
    wrsub             text,
    wrsub_dt          text,
    matched           text default 'n',
    digparts          text,
    digest            text,
    digest_nld        text not null unique,
    actrecord_id      integer, -- from most recent digest_nld found, not unique!
    query_pass        integer default 0,
    primary key(scc_temp_id))
    """
    actstage_index_q = "create index actstage_idx_digest on actstage(digest)"

    cursor = conn.cursor()
    cursor.execute(actstage_create_q)
    cursor.execute(actstage_index_q)

    actstage_index_q2 = "create index actstage_idx_digest2 on actstage(digest_nld)"
    cursor.execute(actstage_index_q2)

    actstage_index_q3 = "create index actstage_idx_actrecord_id on actstage(actrecord_id)"
    actstage_index_q4 = "create index actstage_idx_query_pass on actstage(query_pass)"
    
    cursor.execute(actstage_index_q3)
    cursor.execute(actstage_index_q4)
    conn.commit()

    return

def act_match_table_drop(conn):
    q = "drop table if exists act_match"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def act_match_table_create(conn):
    q = """
        create table if not exists act_match (
        ID integer,
        ACTRECORD_ID integer,
        CATEG text,
        TREC integer,
        EMPLID text,
        LDATE text,
        ACTID text,
        AP_LAST text,
        SUSP_LAST text,
        AP_FIRST text,
        AP_PREF text,
        SUSP_FIRST text,
        AP_MID text,
        SMI text,
        AX text,
        SX text,
        AP_DOB text,
        SUSP_DOB text,
        AP_MA1 text,
        AP_HA1 text,
        AP_SA1 text,
        SUSP_ADDR text,
        AP_MCITY text,
        AP_HCITY text,
        AP_SCITY text,
        SUSP_CITY text,
        AP_MSTATE text,
        AP_HSTATE text,
        AP_SSTATE text,
        SUSP_STATE text,
        AP_MPOSTAL text,
        AP_HPOSTAL text,
        SUSP_POSTAL text,
        AP_MCO text,
        AP_HCO text,
        AP_SCO text,
        SUSP_CO text,
        HPHONE text,
        CPHONE text,
        MOPHONE text,
        FAPHONE text,
        SUSP_PHONE text,
        EMAIL text,
        MOEMAIL text,
        FAEMAIL text,
        SUSP_EMAIL text,
        AP_ATP text,
        SUSP_ATP text,
        RAT text,
        APE text,
        ADR text,
        DOB text,
        PHO text,
        ATP text,
        POS text,
        FN text,
        EM text,
        NPL text,
        ANO text,
        LN text,
        FDUP text,
        PRIMARY KEY (ID ASC))
    """
    cursor = conn.cursor()
    cursor.execute(q)
    conn.commit()
    return

def actfile_table_drop(conn):
    q = "drop table if exists actfile"
    cur = conn.cursor()
    cur.execute(q)
    return

#def actfile_table_create(conn):
#    q = """
#    create table if not exists actfile(
#    actfile_id integer,
#    filename text,
#    loaddate text,
#    lines int,
#    PRIMARY KEY (actfile_id ASC))
#    """
#    cursor = conn.cursor()
#    cursor.execute(q)
#    return

def actfile_table_create(conn):
    q = """
    create table if not exists actfile(
    actfile_id integer,
    filename text,
    altfilename text,
    loaddate text,
    lines int,
    oldactfile_id integer,
    oldfilename text,
    oldloaddate text,
    oldlines int,
    PRIMARY KEY (actfile_id ASC))
    """
    cursor = conn.cursor()
    cursor.execute(q)
    conn.commit()
    return

def actfile2_table_drop(conn):
    q = "drop table if exists actfile2"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

# old data for migrating use
def actfile2_table_create(conn):
    q = """
    create table if not exists actfile2(
    actfile_id integer,
    filename text,
    loaddate text,
    lines int,
    PRIMARY KEY (actfile_id ASC))
    """
    cursor = conn.cursor()
    cursor.execute(q)
    conn.commit()
    return
def actrecord_table_drop(conn):
    q = "drop table if exists actrecord"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_table_create(conn):
    q = """
    create table if not exists actrecord(
    actrecord_id integer,
    actfile_id   integer,
    fileline      int,
    actid         text,
    edob          text,
    genderalpha   text,
    last          text,
    first         text,
    mi            text,
    street        text,
    city          text,
    state         text,
    zip5          text,
    cnprovince    text,
    cnpostal      text,
    country_code  text,
    phonetype     text,
    phone         text,
    gradelevel    text,
    hscode        text,
    hsavg         text,
    gradyr        text,
    etestdate     text,
    testloc       text,
    corrrpt       text,
    engl          text,
    math          text,
    read          text,
    scire         text,
    comp          text,
    ewcomb        text,
    wrsub         text,
    wrdescr       text,
    writ_subj     text, -- 2015
    writ_subj16   text, -- >= 2016
    writ_dom1     text,
    writ_dom2     text,
    writ_dom3     text,
    writ_dom4     text,
    writ_dom_avg  text, --computed added 2017-11-08
    ela_score     text,
    stem_score    text,
    ucomplex_ind  text,
    prog_ind      text,
    engl_norm     text,
    math_norm     text,
    read_norm     text,
    scire_norm    text,
    comp_norm     text,
    ewcombnorm    text,
    wrsubnorm     text,
    writ_norm     text,
    ela_norm      text,
    stem_norm     text,
    reportyear    text,
    stateid       text,
    email         text,
    sup_comp_type text default '',
    digparts      text,
    digest        text,
    digest_nld    text,
    PRIMARY KEY (actrecord_id ASC),
    FOREIGN KEY (actfile_id) REFERENCES actfile(actfile_id))
    """
    cur = conn.cursor()
    cur.execute(q)

    actrecord_index_q = "create index actrecord_idx_digest on actrecord(digest)"
    cur.execute(actrecord_index_q)

    actrecord_index_q2 = "create index actrecord_idx_digest_nld on actrecord(digest_nld)"
    cur.execute(actrecord_index_q2)

    return

def act_matched_keys_table_drop(conn):
    q = "drop table if exists act_matched_keys"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def act_matched_keys_table_create(conn):
    q = """
    create table if not exists act_matched_keys (
    actrecord_id integer,
    siss_load_date text,
    emplid text,
    match_date text,
    scc_temp_id text default '',
    PRIMARY KEY(actrecord_id, emplid, siss_load_date),
    FOREIGN KEY(actrecord_id) REFERENCES actrecord(actrecord_id) ON DELETE CASCADE)

    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def act_exclude_table_drop(conn):
    q = "drop table if exists act_exclude"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def act_exclude_table_create(conn):
    q = """
    create table if not exists act_exclude (
    actrecord_id integer,
    siss_id      text,
    emplid       text,
    excldate     text,
    PRIMARY KEY (actrecord_id, siss_id, emplid),
    FOREIGN KEY (actrecord_id) REFERENCES actrecord(actrecord_id) ON DELETE CASCADE)
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_addl_1_table_drop(conn):
    q = "drop table if exists actrecord_addl_1"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_addl_1_table_create(conn):
    q = """
    create table if not exists actrecord_addl_1 (
        actrecord_id         integer,
        data_src             text,
        rep_dte              text,
        id_act               text,
        coll_cde             text,
        f_name               text,
        m_initial            text,
        l_name               text,
        address1             text,
        address2             text,
        city                 text,
        state_full           text,
        state                text,
        zip5                 text,
        zip4                 text,
        country_txt          text,
        country_iso          text,
        country_cde          text,
        prov                 text,
        postal               text,
        phone                text,
        ph_ctrycde           text,
        email                text,
        dob                  text,
        gender               text,
        hs_code              text,
        hs_name              text,
        hs_gradelev          text,
        test_dte             text,
        test_loc             text,
        id_stateassign       text,
        eng                  text,
        mth                  text,
        rdg                  text,
        sci                  text,
        composite            text,
        stem                 text,
        writing              text,
        ela                  text,
        sup_eng_dt           text,
        sup_eng_loc          text,
        sup_eng_sc           text,
        sup_otheng           text,
        sup_eng_type         text,
        sup_mth_dt           text,
        sup_mth_loc          text,
        sup_mth_sc           text,
        sup_othmth           text,
        sup_mth_type         text,
        sup_rdg_dt           text,
        sup_rdg_loc          text,
        sup_rdg_sc           text,
        sup_othrdg           text,
        sup_rdg_type         text,
        sup_sci_dt           text,
        sup_sci_loc          text,
        sup_sci_sc           text,
        sup_othsci           text,
        sup_sci_type         text,
        sup_composite        text,
        sup_stem             text,
        sup_writ_dt          text,
        sup_writ_loc         text,
        sup_writ_sc          text,
        sup_writ_type        text,
        sup_ela              text,
        w_dom_ideas          text,
        w_dom_supt           text,
        w_dom_org            text,
        w_dom_languse        text,
        PRIMARY KEY (actrecord_id),
        FOREIGN KEY (actrecord_id) REFERENCES actrecord(actrecord_id) ON DELETE CASCADE)
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_addl_2_table_drop(conn):
    q = "drop table if exists actrecord_addl_2"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_addl_2_table_create(conn):
    q = """
    create table if not exists actrecord_addl_2 (
    actrecord_id          integer,
    fips                  text,
    county_name           text,
    ph_type               text,
    ethn_hisplat          text,
    ethn_alaskanat        text,
    ethn_asian            text,
    ethn_africamer        text,
    ethn_pacisland        text,
    ethn_white            text,
    ethn_prefnorsp        text,
    ethn_fedvalues        text,
    hs_gradyr             text,
    hs_type               text,
    coll_choicnum         text,
    major                 text,
    major_code            text,
    cip_code              text,
    major_certainty       text,
    voc                   text,
    voc_code              text,
    voc_certainty         text,
    intmaj_fit            text,
    single_hightest       text,
    sum_scale             text,
    full_part             text,
    live_pln              text,
    highest_ed_exp        text,
    rotc                  text,
    help_edplan           text,
    help_writing          text,
    help_reading          text,
    help_studysk          text,
    help_mathsk           text,
    want_indepst          text,
    want_honors           text,
    want_studyab          text,
    plan_instmus          text,
    plan_vocmus           text,
    plan_stugov           text,
    plan_pubs             text,
    plan_debate           text,
    plan_theater          text,
    plan_relorg           text,
    plan_racethorg        text,
    plan_vathl            text,
    plan_polorg           text,
    plan_radiotv          text,
    plan_fratsor          text,
    plan_servorg          text,
    exp_finaidapp         text,
    exp_work              text,
    exp_workhrs           text,
    fam_income            text,
    edlevel_mthr          text,
    edlevel_fthr          text,
    pref_dist             text,
    insttypepref          text,
    insttypepref_rnk      text,
    malefempref           text,
    malefempref_rnk       text,
    statepref1            text,
    statepref2            text,
    statepref_rnk         text,
    costpref              text,
    costpref_rnk          text,
    sizepref              text,
    sizepref_rnk          text,
    acadmajpref_rnk       text,
    otherfac_rnk          text,
    rel_int               text,
    hs_size               text,
    hs_classrnk           text,
    hs_gpa_range          text,
    hs_curriculum         text,
    hs_gpa_calc           text,
    hs_yrs_eng            text,
    hs_yrs_mth            text,
    hs_yrs_soc            text,
    hs_yrs_natsc          text,
    hs_yrs_span           text,
    hs_yrs_germ           text,
    PRIMARY KEY (actrecord_id),
    FOREIGN KEY (actrecord_id) REFERENCES actrecord(actrecord_id) ON DELETE CASCADE)
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_addl_3_table_drop(conn):
    q = "drop table if exists actrecord_addl_3"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_addl_3_table_create(conn):
    q = """
    create table if not exists actrecord_addl_3 (
    actrecord_id              integer,
    hs_yrs_frch               text,
    hs_yrs_othlang            text,
    hons_eng                  text,
    hons_mth                  text,
    hons_soc                  text,
    hons_natsc                text,
    hons_lang                 text,
    activ_instmus             text,
    activ_vocmus              text,
    activ_stugov              text,
    activ_pubs                text,
    activ_debate              text,
    activ_theater             text,
    activ_relorg              text,
    activ_racethorg           text,
    activ_vathl               text,
    activ_polorg              text,
    activ_radiotv             text,
    activ_socialclb           text,
    activ_servorg             text,
    c_eng_9                   text,
    c_eng_10                  text,
    c_eng_11                  text,
    c_eng_12                  text,
    c_eng_oth                 text,
    c_alg1                    text,
    c_alg2                    text,
    c_geom                    text,
    c_trig                    text,
    c_begcalc                 text,
    c_advmth                  text,
    c_cmpt                    text,
    c_gensc                   text,
    c_bio                     text,
    c_chem                    text,
    c_phys                    text,
    c_ushist                  text,
    c_wrldhist                text,
    c_othhist                 text,
    c_amgov                   text,
    c_econ                    text,
    c_geog                    text,
    c_psych                   text,
    c_span                    text,
    c_frch                    text,
    c_germ                    text,
    c_lang                    text,
    c_art                     text,
    c_mus                     text,
    c_dram                    text,
    g_eng_9                   text,
    g_eng_10                  text,
    g_eng_11                  text,
    g_eng_12                  text,
    g_eng_oth                 text,
    g_alg1                    text,
    g_alg2                    text,
    g_geom                    text,
    g_trig                    text,
    g_begcalc                 text,
    g_advmth                  text,
    g_cmpt                    text,
    g_gensc                   text,
    g_bio                     text,
    g_chem                    text,
    g_phys                    text,
    g_ushist                  text,
    g_wrldhist                text,
    g_othhist                 text,
    g_amgov                   text,
    g_econ                    text,
    g_geog                    text,
    g_psych                   text,
    g_span                    text,
    g_frch                    text,
    g_germ                    text,
    g_lang                    text,
    g_art                     text,
    g_mus                     text,
    g_dram                    text,
    PRIMARY KEY (actrecord_id),
    FOREIGN KEY (actrecord_id) REFERENCES actrecord(actrecord_id) ON DELETE CASCADE)
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_addl_4_table_drop(conn):
    q = "drop table if exists actrecord_addl_4"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_addl_4_table_create(conn):
    q = """
    create table if not exists actrecord_addl_4 (
        actrecord_id           integer,
        g_alleng               text,
        g_allmth               text,
        g_allsocsc             text,
        g_allnatsc             text,
        g_overall              text,
        intinv_scistd          text,
        intinv_scirnk          text,
        intinv_artstd          text,
        intinv_artrnk          text,
        intinv_socstd          text,
        intinv_socrnk          text,
        intinv_buscstd         text,
        intinv_buscrank        text,
        intinv_busopstd        text,
        intinv_busoprnk        text,
        intinv_techstd         text,
        intinv_techrnk         text,
        intinv_mapreg1         text,
        intinv_mapreg2         text,
        intinv_mapreg3         text,
        cs_grp1_rnk            text,
        cs_grp2_rnk            text,
        cs_grp3_rnk            text,
        cs_grp4_rnk            text,
        cs_grp5_rnk            text,
        sub_grp1_rnk           text,
        sub_grp2_rnk           text,
        sub_grp3_rnk           text,
        sub_grp4_rnk           text,
        sub_grp5_rnk           text,
        instrnk_engl           text,
        instrnk_math           text,
        instrnk_read           text,
        instrnk_sci            text,
        instrnk_stem           text,
        instrnk_ela            text,
        rnks_type              text,
        instrnk_writ           text,
        instrnk_enrcomp        text,
        c_readiness            text,
        e_earn_prod            text,
        e_poss_prod            text,
        e_pctc_prod            text,
        e_rdyrng_prod          text,
        e_earn_knlg            text,
        e_poss_knlg            text,
        e_pctc_knlg            text,
        e_rdyrng_knlg          text,
        e_earn_conv            text,
        e_poss_conv            text,
        e_pctc_conv            text,
        e_rdyrng_conv          text,
        m_earn_higher          text,
        m_poss_higher          text,
        m_pctc_higher          text,
        m_rdyrng_higher        text,
        m_earn_numq            text,
        m_poss_numq            text,
        m_pctc_numq            text,
        m_rdyrng_numq          text,
        m_earn_alg             text,
        m_poss_alg             text,
        m_pctc_alg             text,
        m_rdyrng_alg           text,
        m_earn_func            text,
        m_poss_func            text,
        m_pctc_func            text,
        m_rdyrng_func          text,
        m_earn_geom            text,
        m_poss_geom            text,
        m_pctc_geom            text,
        m_rdyrng_geom          text,
        m_earn_stat            text,
        m_poss_stat            text,
        m_pctc_stat            text,
        m_rdyrng_stat          text,
        m_earn_essen           text,
        m_poss_essen           text,
        m_pctc_essen           text,
        m_rdyrng_essen         text,
    PRIMARY KEY (actrecord_id),
    FOREIGN KEY (actrecord_id) REFERENCES actrecord(actrecord_id) ON DELETE CASCADE)
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_addl_5_table_drop(conn):
    q = "drop table if exists actrecord_addl_5"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actrecord_addl_5_table_create(conn):
    q = """
    create table if not exists actrecord_addl_5 (
        actrecord_id      integer,
        m_earn_mod          text,
        m_poss_mod          text,
        m_pctc_mod          text,
        m_rdyrng_mod        text,
        r_earn_ideas        text,
        r_poss_ideas        text,
        r_pctc_ideas        text,
        r_rdyrng_ideas      text,
        r_earn_struc        text,
        r_poss_struc        text,
        r_pctc_struc        text,
        r_rdyrng_struc      text,
        r_earn_knlg         text,
        r_poss_knlg         text,
        r_pctc_knlg         text,
        r_rdyrng_knlg       text,
        r_und_comp_txt      text,
        s_earn_data         text,
        s_poss_data         text,
        s_pctc_data         text,
        s_rdyrng_data       text,
        s_earn_inv          text,
        s_poss_inv          text,
        s_pctc_inv          text,
        s_rdyrng_inv        text,
        s_earn_eval         text,
        s_poss_eval         text,
        s_pctc_eval         text,
        s_rdyrng_eval       text,
        usrnk_sub_engl      text,
        usrnk_sub_math      text,
        usrnk_sub_rdg       text,
        usrnk_sub_sci       text,
        usrnk_comp          text,
        usrnk_stem          text,
        usrnk_wri           text,
        usrnk_ela           text,
        prob_c_group1       text,
        prob_c_group2       text,
        prob_c_group3       text,
        prob_c_group4       text,
        prob_c_group5       text,
        prob_c_course1      text,
        prob_c_course2      text,
        prob_c_course3      text,
        prob_c_course4      text,
        prob_c_course5      text,
        prob_b_group1       text,
        prob_b_group2       text,
        prob_b_group3       text,
        prob_b_group4       text,
        prob_b_group5       text,
        prob_b_course1      text,
        prob_b_course2      text,
        prob_b_course3      text,
        prob_b_course4      text,
        prob_b_course5      text,
        eos_optin           text,
        id_local            text,
        hist_eng            text,
        hist_math           text,
        hist_socst          text,
        hist_natsc          text,
        hist_comp           text,
    PRIMARY KEY (actrecord_id),
    FOREIGN KEY (actrecord_id) REFERENCES actrecord(actrecord_id) ON DELETE CASCADE)
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def actfile2_load_dates_table_create(conn):
    q = """
    create table if not exists actfile2_load_dates(
    actfile2_id text,
    filename text,
    loaddate text,
    lines int,
    primary key (actfile2_id))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def act_local_susp_table_drop(conn):
    q = "drop table if exists act_local_susp"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

# not used
def act_local_susp_table_create(conn):
    q = """
    create table if not exists act_local_susp (
          actrecord_id        integer,
          siss_id             text defailt '',
          emplid              text default '',
          actid               text,
          last                text,
          first               text,
          mi                  text,
          gender              text,
          dob                 text,
          street              text,
          city                text,
          state               text,
          postal              text,
          country             text,
          email               text,
          phone               text,
          hscode              text,
          etestdate           text,
          engl                text,
          math                text,
          read                text,
          scire               text,
          comp                text,
          stateid             text,
          loaddate            text,
          digparts            text,
          digest              text,
          digest_nld          text,
          matched             boolean default false,
          PRIMARY KEY (actrecord_id),
          FOREIGN KEY (actrecord_id) REFERENCES actrecord(actrecord_id) ON DELETE CASCADE)
    """
    cur = conn.cursor()
    cur.execute(q)

    act_ls_index_q = "create index act_local_susp_digest on act_local_susp(digest)"
    cur.execute(act_ls_index_q)

    act_ls_index_q2 = "create index act_local_susp_digest_nld  on act_local_susp(digest_nld)"
    cur.execute(act_ls_index_q2)
    conn.commit()

    return

def satfile_table_drop(conn):
    q = """
    drop table if exists satfile
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()

def satfile_table_create(conn):
    q = """
    create table if not exists satfile(
    satfile_id integer,
    filename text,
    altfilename text,
    loaddate text,
    lines int,
    oldsatfile_id integer,
    oldfilename text,
    oldloaddate text,
    oldlines int,
    PRIMARY KEY (satfile_id ASC))
    """

    q2 = "create index satfile_idx_loaddate on satfile(loaddate)"
    q3 = "create index satfile_idx_max_loaddate on satfile(max(loaddate))"
    cur = conn.cursor()
    cur.execute(q)
    cur.execute(q2)
    cur.execute(q3)
    conn.commit()

def satrecord_table_drop(conn):
    q = """
    drop table if exists satrecord
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()

def satrecord_table_create(conn):
    q = """
    create table satrecord (
    satrecord_id integer,
    satfile_id integer,
    fileline integer,
    last text,
    first text,
    mi text,
    sex text,
    dob text,
    nid text,
    addr1 text,
    addr2 text,
    city text,
    state text,
    postal text,
    country2 text,
    country3 text,
    province text,
    phone text,
    email text,
    graddate text,
    fai text,
    tdate_1 text,
    glevel_1 text,
    rsi_1 text,
    total_1 text,
    erws_1 text,
    mss_1 text,
    rt_1 text,
    wlt_1 text,
    mt_1 text,
    asc_1 text,
    ahssc_1 text,
    rwc_1 text,
    ce_1 text,
    ei_1 text,
    sec_1 text,
    ha_1 text,
    pam_1 text,
    psda_1 text,
    esr_1 text,
    esa_1 text,
    esw_1 text,
    verb_1 text,
    math_1 text,
    wrsc_1 text,
    ess_1 text,
    mc_1 text,
    essay_1 text,
    tdate_2 text,
    glevel_2 text,
    rsi_2 text,
    total_2 text,
    erws_2 text,
    mss_2 text,
    rt_2 text,
    wlt_2 text,
    mt_2 text,
    asc_2 text,
    ahssc_2 text,
    rwc_2 text,
    ce_2 text,
    ei_2 text,
    sec_2 text,
    ha_2 text,
    pam_2 text,
    psda_2 text,
    esr_2 text,
    esa_2 text,
    esw_2 text,
    verb_2 text,
    math_2 text,
    wrsc_2 text,
    ess_2 text,
    mc_2 text,
    essay_2 text,
    tdate_3 text,
    glevel_3 text,
    rsi_3 text,
    total_3 text,
    erws_3 text,
    mss_3 text,
    rt_3 text,
    wlt_3 text,
    mt_3 text,
    asc_3 text,
    ahssc_3 text,
    rwc_3 text,
    ce_3 text,
    ei_3 text,
    sec_3 text,
    ha_3 text,
    pam_3 text,
    psda_3 text,
    esr_3 text,
    esa_3 text,
    esw_3 text,
    verb_3 text,
    math_3 text,
    wrsc_3 text,
    ess_3 text,
    mc_3 text,
    essay_3 text,
    digparts      text,
    digest        text,
    digest_nld    text,
    PRIMARY KEY (satrecord_id ASC),
    FOREIGN KEY (satfile_id) REFERENCES satfile(satfile_id))
    """

    q2 = "create index satrecord_idx_satfile_id on satrecord(satfile_id)"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    
    
def satrecord_addl_1_table_drop(conn):
    q = """
    drop table if exists satrecord_addl_1
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()

def satrecord_addl_1_table_create(conn):
    q = """
    create table if not exists satrecord_addl_1 (
    satrecord_id integer,
    tdate_4 text,
    glevel_4 text,
    rsi_4 text,
    total_4 text,
    erws_4 text,
    mss_4 text,
    rt_4 text,
    wlt_4 text,
    mt_4 text,
    asc_4 text,
    ahssc_4 text,
    rwc_4 text,
    ce_4 text,
    ei_4 text,
    sec_4 text,
    ha_4 text,
    pam_4 text,
    psda_4 text,
    esr_4 text,
    esa_4 text,
    esw_4 text,
    verb_4 text,
    math_4 text,
    wrsc_4 text,
    ess_4 text,
    mc_4 text,
    essay_4 text,
    tdate_5 text,
    glevel_5 text,
    rsi_5 text,
    total_5 text,
    erws_5 text,
    mss_5 text,
    rt_5 text,
    wlt_5 text,
    mt_5 text,
    asc_5 text,
    ahssc_5 text,
    rwc_5 text,
    ce_5 text,
    ei_5 text,
    sec_5 text,
    ha_5 text,
    pam_5 text,
    psda_5 text,
    esr_5 text,
    esa_5 text,
    esw_5 text,
    verb_5 text,
    math_5 text,
    wrsc_5 text,
    ess_5 text,
    mc_5 text,
    essay_5 text,
    tdate_6 text,
    glevel_6 text,
    rsi_6 text,
    total_6 text,
    erws_6 text,
    mss_6 text,
    rt_6 text,
    wlt_6 text,
    mt_6 text,
    asc_6 text,
    ahssc_6 text,
    rwc_6 text,
    ce_6 text,
    ei_6 text,
    sec_6 text,
    ha_6 text,
    pam_6 text,
    psda_6 text,
    esr_6 text,
    esa_6 text,
    esw_6 text,
    verb_6 text,
    math_6 text,
    wrsc_6 text,
    ess_6 text,
    mc_6 text,
    essay_6 text,
    PRIMARY KEY (satrecord_id),
    FOREIGN KEY (satrecord_id) REFERENCES satrecord(satrecord_id) ON DELETE CASCADE)
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    
def satrecord_addl_2_table_drop(conn):
    q = """
    drop table if exists satrecord_addl_2
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()

def satrecord_addl_2_table_create(conn):
    q = """
    create table if not exists satrecord_addl_2 (
    satrecord_id integer,
    jtdate_1 text,
    jglevel_1 text,
    jrsi_1 text,
    jt1code_1 text,
    jt1alph_1 text,
    jt1score_1 text,
    jt1ss1_1 text,
    jt1ss2_1 text,
    jt1ss3_1 text,
    jt2code_1 text,
    jt2alph_1 text,
    jt2score_1 text,
    jt2ss1_1 text,
    jt2ss2_1 text,
    jt2ss3_1 text,
    jt3code_1 text,
    jt3alph_1 text,
    jt3score_1 text,
    jt3ss1_1 text,
    jt3ss2_1 text,
    jt3ss3_1 text,
    jtdate_2 text,
    jglevel_2 text,
    jrsi_2 text,
    jt1code_2 text,
    jt1alph_2 text,
    jt1score_2 text,
    jt1ss1_2 text,
    jt1ss2_2 text,
    jt1ss3_2 text,
    jt2code_2 text,
    jt2alph_2 text,
    jt2score_2 text,
    jt2ss1_2 text,
    jt2ss2_2 text,
    jt2ss3_2 text,
    jt3code_2 text,
    jt3alph_2 text,
    jt3score_2 text,
    jt3ss1_2 text,
    jt3ss2_2 text,
    jt3ss3_2 text,
    jtdate_3 text,
    jglevel_3 text,
    jrsi_3 text,
    jt1code_3 text,
    jt1alph_3 text,
    jt1score_3 text,
    jt1ss1_3 text,
    jt1ss2_3 text,
    jt1ss3_3 text,
    jt2code_3 text,
    jt2alph_3 text,
    jt2score_3 text,
    jt2ss1_3 text,
    jt2ss2_3 text,
    jt2ss3_3 text,
    jt3code_3 text,
    jt3alph_3 text,
    jt3score_3 text,
    jt3ss1_3 text,
    jt3ss2_3 text,
    jt3ss3_3 text,
    jtdate_4 text,
    jglevel_4 text,
    jrsi_4 text,
    jt1code_4 text,
    jt1alph_4 text,
    jt1score_4 text,
    jt1ss1_4 text,
    jt1ss2_4 text,
    jt1ss3_4 text,
    jt2code_4 text,
    jt2alph_4 text,
    jt2score_4 text,
    jt2ss1_4 text,
    jt2ss2_4 text,
    jt2ss3_4 text,
    jt3code_4 text,
    jt3alph_4 text,
    jt3score_4 text,
    jt3ss1_4 text,
    jt3ss2_4 text,
    jt3ss3_4 text,
    jtdate_5 text,
    jglevel_5 text,
    jrsi_5 text,
    jt1code_5 text,
    jt1alph_5 text,
    jt1score_5 text,
    jt1ss1_5 text,
    jt1ss2_5 text,
    jt1ss3_5 text,
    jt2code_5 text,
    jt2alph_5 text,
    jt2score_5 text,
    jt2ss1_5 text,
    jt2ss2_5 text,
    jt2ss3_5 text,
    jt3code_5 text,
    jt3alph_5 text,
    jt3score_5 text,
    jt3ss1_5 text,
    jt3ss2_5 text,
    jt3ss3_5 text,
    jtdate_6 text,
    jglevel_6 text,
    jrsi_6 text,
    jt1code_6 text,
    jt1alph_6 text,
    jt1score_6 text,
    jt1ss1_6 text,
    jt1ss2_6 text,
    jt1ss3_6 text,
    jt2code_6 text,
    jt2alph_6 text,
    jt2score_6 text,
    jt2ss1_6 text,
    jt2ss2_6 text,
    jt2ss3_6 text,
    jt3code_6 text,
    jt3alph_6 text,
    jt3score_6 text,
    jt3ss1_6 text,
    jt3ss2_6 text,
    jt3ss3_6 text,
    hscode text,
    hsname text,
    hsa1 text,
    hsa2 text,
    hsa3 text,
    hscity text,
    hsstate text,
    hscountry2 text,
    hsprovince text,
    hsphone text,
    hstype text,
    reportdate text,
    raceblock text,
    drace text,
    cbsid text,
    sssid text,
    cntyfips text,
    hsi text,
    PRIMARY KEY (satrecord_id),
    FOREIGN KEY (satrecord_id) REFERENCES satrecord(satrecord_id) ON DELETE CASCADE)
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    
def sat_exclude_table_drop(conn):
    q = "drop table if exists sat_exclude"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def sat_exclude_table_create(conn):
    q = """
    create table if not exists sat_exclude (
    satrecord_id integer,
    siss_id      text,
    emplid       text,
    excldate     text,
    PRIMARY KEY (satrecord_id, siss_id, emplid),
    FOREIGN KEY (satrecord_id) REFERENCES satrecord(satrecord_id) ON DELETE CASCADE)
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def sat_matched_keys_table_drop(conn):
    q = "drop table if exists sat_matched_keys"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def sat_matched_keys_table_create(conn):
    q = """
    create table if not exists sat_matched_keys (
    satrecord_id integer,
    siss_load_date text,
    emplid text,
    match_date text,
    scc_temp_id text default '',
    PRIMARY KEY(satrecord_id, emplid, siss_load_date),
    FOREIGN KEY(satrecord_id) REFERENCES satrecord(satrecord_id) ON DELETE CASCADE)

    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def sat_match_table_drop(conn):
    q = "drop table if exists sat_match"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def sat_match_table_create(conn):
    q = """
        create table if not exists sat_match (
        ID integer,
        SATRECORD_ID integer,
        CATEG text,
        TREC integer,
        EMPLID text,
        LDATE text,
        AP_LAST text,
        SUSP_LAST text,
        AP_FIRST text,
        AP_PREF text,
        SUSP_FIRST text,
        AP_MID text,
        SMI text,
        AX text,
        SX text,
        AP_DOB text,
        SUSP_DOB text,
        AP_MA1 text,
        AP_HA1 text,
        AP_SA1 text,
        SUSP_ADDR text,
        AP_MCITY text,
        AP_HCITY text,
        AP_SCITY text,
        SUSP_CITY text,
        AP_MSTATE text,
        AP_HSTATE text,
        AP_SSTATE text,
        SUSP_STATE text,
        AP_MPOSTAL text,
        AP_HPOSTAL text,
        SUSP_POSTAL text,
        AP_MCO text,
        AP_HCO text,
        AP_SCO text,
        SUSP_CO text,
        AP_CEEB text,
        SUSP_CEEB text,
        AP_SNAME text,
        SUSP_SNAME text,
        HPHONE text,
        CPHONE text,
        MOPHONE text,
        FAPHONE text,
        SUSP_PHONE text,
        EMAIL text,
        MOEMAIL text,
        FAEMAIL text,
        SUSP_EMAIL text,
        RAT text,
        APE text,
        ADR text,
        DOB text,
        PHO text,
        CEEB text,
        POS text,
        FN text,
        EM text,
        NPL text,
        ANO text,
        LN text,
        FDUP text,
        PRIMARY KEY (ID ASC))
    """
    cursor = conn.cursor()
    cursor.execute(q)
    conn.commit()
    return

def satstage_table_drop(conn):
    q = "drop table if exists satstage"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def satstage_table_create(conn):
    q = """
    create table if not exists satstage(
    scc_temp_id      integer,
    test_id          text,
    ls_data_source   text,
    date_loaded      text,
    last_name        text default '',
    first_name       text default '',
    middle_name      text default '',
    email_addr       text default '',
    address1         text,
    city             text,
    state            text,
    postal           text,
    country          text,
    birthdate        text,
    phone            text,
    status           text,
    created          text,
    lastupd          text,
    emplid           text,
    erws       text,
    erws_dt    text,
    mss        text,
    mss_dt     text,
    total      text,
    total_dt   text,
    digest     text,
    digparts   text,
    digest_nld text not null unique,
    satrecord_id  integer, -- from most recent digest_nld found, not unique!
    query_pass    integer default 0,
    primary key (scc_temp_id))
    """

    q2 = "create index satstage_idx_digest_nld on satstage(digest_nld)"

    cur = conn.cursor()
    cur.execute(q)

    cur = conn.cursor()
    cur.execute(q2)
    conn.commit()


    return

def sat_lu_table_drop(conn):
    q = "drop table if exists sat_lu"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def sat_lu_table_create(conn):
    q = """
    create table if not exists sat_lu (
    ncode text,
    acode text,
    primary key (ncode))
    """
    q_ins = """
    insert into sat_lu (ncode, acode) values
      ('39','UH'),
      ('40','WH'),
      ('41','LR'),
      ('43','CH'),
      ('44','PH'),
      ('45','LT'),
      ('46','MH'),
      ('47','FR'),
      ('48','GM'),
      ('49','IT'),
      ('51','SP'),
      ('52','M2'),
      ('54','CL'),
      ('55','FL'),
      ('56','GL'),
      ('57','JL'),
      ('58','SL'),
      ('59','KL'),
      ('61','M1'),
      ('62','EB'),
      ('63','MB')
    """
    cur = conn.cursor()
    for val in [q,q_ins]:
        cur.execute(val)
    conn.commit()
        
def sat_country_lu_table_drop(conn):
    q = "drop table if exists sat_country_lu"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return
    
def sat_country_lu_table_create(conn):
    q = """
    create table if not exists sat_country_lu (
    name text,
    code2 text,
    code3 text,
    primary key (code2))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    
def sat_country_lu_table_load(conn):
    q = """
    insert into sat_country_lu (name,code2,code3) values
      ('BLANK','',''),
      ('Afghanistan','AF','AFG'),
      ('Aland Islands','AX','ALA'),
      ('Albania','AL','ALB'),
      ('Algeria','DZ','DZA'),
      ('American Samoa','AS','ASM'),
      ('Andorra','AD','AND'),
      ('Angola','AO','AGO'),
      ('Anguilla','AI','AIA'),
      ('Antarctica','AQ','ATA'),
      ('Antigua and Barbuda','AG','ATG'),
      ('Argentina','AR','ARG'),
      ('Armenia','AM','ARM'),
      ('Aruba','AW','ABW'),
      ('Australia','AU','AUS'),
      ('Austria','AT','AUT'),
      ('Azerbaijan','AZ','AZE'),
      ('Bahamas','BS','BHS'),
      ('Bahrain','BH','BHR'),
      ('Bangladesh','BD','BGD'),
      ('Barbados','BB','BRB'),
      ('Belarus','BY','BLR'),
      ('Belgium','BE','BEL'),
      ('Belize','BZ','BLZ'),
      ('Benin','BJ','BEN'),
      ('Bermuda','BM','BMU'),
      ('Bhutan','BT','BTN'),
      ('Bolivia, Plurinational State of','BO','BOL'),
      ('Bonaire, Sint Eustatius and Saba','BQ','BES'),
      ('Bosnia and Herzegovina','BA','BIH'),
      ('Botswana','BW','BWA'),
      ('Bouvet Island','BV','BVT'),
      ('Brazil','BR','BRA'),
      ('British Indian Ocean Territory','IO','IOT'),
      ('Brunei Darussalam','BN','BRN'),
      ('Bulgaria','BG','BGR'),
      ('Burkina Faso','BF','BFA'),
      ('Burundi','BI','BDI'),
      ('Cabo Verde','CV','CPV'),
      ('Cambodia','KH','KHM'),
      ('Cameroon','CM','CMR'),
      ('Canada','CA','CAN'),
      ('Cayman Islands','KY','CYM'),
      ('Central African Republic','CF','CAF'),
      ('Chad','TD','TCD'),
      ('Chile','CL','CHL'),
      ('China','CN','CHN'),
      ('Christmas Island','CX','CXR'),
      ('Cocos (Keeling) Islands','CC','CCK'),
      ('Colombia','CO','COL'),
      ('Comoros','KM','COM'),
      ('Congo','CG','COG'),
      ('Congo, the Democratic Republic of the','CD','COD'),
      ('Cook Islands','CK','COK'),
      ('Costa Rica','CR','CRI'),
      ('Cote d''Ivoire','CI','CIV'),
      ('Croatia','HR','HRV'),
      ('Cuba','CU','CUB'),
      ('Curacao','CW','CUW'),
      ('Cyprus','CY','CYP'),
      ('Czech Republic','CZ','CZE'),
      ('Denmark','DK','DNK'),
      ('Djibouti','DJ','DJI'),
      ('Dominica','DM','DMA'),
      ('Dominican Republic','DO','DOM'),
      ('Ecuador','EC','ECU'),
      ('Egypt','EG','EGY'),
      ('El Salvador','SV','SLV'),
      ('Equatorial Guinea','GQ','GNQ'),
      ('Eritrea','ER','ERI'),
      ('Estonia','EE','EST'),
      ('Ethiopia','ET','ETH'),
      ('Falkland Islands (Malvinas)','FK','FLK'),
      ('Faroe Islands','FO','FRO'),
      ('Fiji','FJ','FJI'),
      ('Finland','FI','FIN'),
      ('France','FR','FRA'),
      ('French Guiana','GF','GUF'),
      ('French Polynesia','PF','PYF'),
      ('French Southern Territories','TF','ATF'),
      ('Gabon','GA','GAB'),
      ('Gambia','GM','GMB'),
      ('Georgia','GE','GEO'),
      ('Germany','DE','DEU'),
      ('Ghana','GH','GHA'),
      ('Gibraltar','GI','GIB'),
      ('Greece','GR','GRC'),
      ('Greenland','GL','GRL'),
      ('Grenada','GD','GRD'),
      ('Guadeloupe','GP','GLP'),
      ('Guam','GU','GUM'),
      ('Guatemala','GT','GTM'),
      ('Guernsey','GG','GGY'),
      ('Guinea','GN','GIN'),
      ('Guinea-Bissau','GW','GNB'),
      ('Guyana','GY','GUY'),
      ('Haiti','HT','HTI'),
      ('Heard Island and McDonald Islands','HM','HMD'),
      ('Holy See','VA','VAT'),
      ('Honduras','HN','HND'),
      ('Hong Kong','HK','HKG'),
      ('Hungary','HU','HUN'),
      ('Iceland','IS','ISL'),
      ('India','IN','IND'),
      ('Indonesia','ID','IDN'),
      ('Iran, Islamic Republic of','IR','IRN'),
      ('Iraq','IQ','IRQ'),
      ('Ireland','IE','IRL'),
      ('Isle of Man','IM','IMN'),
      ('Israel','IL','ISR'),
      ('Italy','IT','ITA'),
      ('Jamaica','JM','JAM'),
      ('Japan','JP','JPN'),
      ('Jersey','JE','JEY'),
      ('Jordan','JO','JOR'),
      ('Kazakhstan','KZ','KAZ'),
      ('Kenya','KE','KEN'),
      ('Kiribati','KI','KIR'),
      ('Korea, Democratic People''s Republic of','KP','PRK'),
      ('Korea, Republic of','KR','KOR'),
      ('Kuwait','KW','KWT'),
      ('Kyrgyzstan','KG','KGZ'),
      ('Lao People''s Democratic Republic','LA','LAO'),
      ('Latvia','LV','LVA'),
      ('Lebanon','LB','LBN'),
      ('Lesotho','LS','LSO'),
      ('Liberia','LR','LBR'),
      ('Libya','LY','LBY'),
      ('Liechtenstein','LI','LIE'),
      ('Lithuania','LT','LTU'),
      ('Luxembourg','LU','LUX'),
      ('Macao','MO','MAC'),
      ('Macedonia, the former Yugoslav Republic of','MK','MKD'),
      ('Madagascar','MG','MDG'),
      ('Malawi','MW','MWI'),
      ('Malaysia','MY','MYS'),
      ('Maldives','MV','MDV'),
      ('Mali','ML','MLI'),
      ('Malta','MT','MLT'),
      ('Marshall Islands','MH','MHL'),
      ('Martinique','MQ','MTQ'),
      ('Mauritania','MR','MRT'),
      ('Mauritius','MU','MUS'),
      ('Mayotte','YT','MYT'),
      ('Mexico','MX','MEX'),
      ('Micronesia, Federated States of','FM','FSM'),
      ('Moldova, Republic of','MD','MDA'),
      ('Monaco','MC','MCO'),
      ('Mongolia','MN','MNG'),
      ('Montenegro','ME','MNE'),
      ('Montserrat','MS','MSR'),
      ('Morocco','MA','MAR'),
      ('Mozambique','MZ','MOZ'),
      ('Myanmar','MM','MMR'),
      ('Namibia','NA','NAM'),
      ('Nauru','NR','NRU'),
      ('Nepal','NP','NPL'),
      ('Netherlands','NL','NLD'),
      ('New Caledonia','NC','NCL'),
      ('New Zealand','NZ','NZL'),
      ('Nicaragua','NI','NIC'),
      ('Niger','NE','NER'),
      ('Nigeria','NG','NGA'),
      ('Niue','NU','NIU'),
      ('Norfolk Island','NF','NFK'),
      ('Northern Mariana Islands','MP','MNP'),
      ('Norway','NO','NOR'),
      ('Oman','OM','OMN'),
      ('Pakistan','PK','PAK'),
      ('Palau','PW','PLW'),
      ('Palestine, State of','PS','PSE'),
      ('Panama','PA','PAN'),
      ('Papua New Guinea','PG','PNG'),
      ('Paraguay','PY','PRY'),
      ('Peru','PE','PER'),
      ('Philippines','PH','PHL'),
      ('Pitcairn','PN','PCN'),
      ('Poland','PL','POL'),
      ('Portugal','PT','PRT'),
      ('Puerto Rico','PR','PRI'),
      ('Qatar','QA','QAT'),
      ('Reunion','RE','REU'),
      ('Romania','RO','ROU'),
      ('Russian Federation','RU','RUS'),
      ('Rwanda','RW','RWA'),
      ('Saint Barthelemy','BL','BLM'),
      ('Saint Helena, Ascension and Tristan da Cunha','SH','SHN'),
      ('Saint Kitts and Nevis','KN','KNA'),
      ('Saint Lucia','LC','LCA'),
      ('Saint Martin (French part)','MF','MAF'),
      ('Saint Pierre and Miquelon','PM','SPM'),
      ('Saint Vincent and the Grenadines','VC','VCT'),
      ('Samoa','WS','WSM'),
      ('San Marino','SM','SMR'),
      ('Sao Tome and Principe','ST','STP'),
      ('Saudi Arabia','SA','SAU'),
      ('Senegal','SN','SEN'),
      ('Serbia','RS','SRB'),
      ('Seychelles','SC','SYC'),
      ('Sierra Leone','SL','SLE'),
      ('Singapore','SG','SGP'),
      ('Sint Maarten (Dutch part)','SX','SXM'),
      ('Slovakia','SK','SVK'),
      ('Slovenia','SI','SVN'),
      ('Solomon Islands','SB','SLB'),
      ('Somalia','SO','SOM'),
      ('South Africa','ZA','ZAF'),
      ('South Georgia and the South Sandwich Islands','GS','SGS'),
      ('South Sudan','SS','SSD'),
      ('Spain','ES','ESP'),
      ('Sri Lanka','LK','LKA'),
      ('Sudan','SD','SDN'),
      ('Suriname','SR','SUR'),
      ('Svalbard and Jan Mayen','SJ','SJM'),
      ('Swaziland','SZ','SWZ'),
      ('Sweden','SE','SWE'),
      ('Switzerland','CH','CHE'),
      ('Syrian Arab Republic','SY','SYR'),
      ('Taiwan, Province of China','TW','TWN'),
      ('Tajikistan','TJ','TJK'),
      ('Tanzania, United Republic of','TZ','TZA'),
      ('Thailand','TH','THA'),
      ('Timor-Leste','TL','TLS'),
      ('Togo','TG','TGO'),
      ('Tokelau','TK','TKL'),
      ('Tonga','TO','TON'),
      ('Trinidad and Tobago','TT','TTO'),
      ('Tunisia','TN','TUN'),
      ('Turkey','TR','TUR'),
      ('Turkmenistan','TM','TKM'),
      ('Turks and Caicos Islands','TC','TCA'),
      ('Tuvalu','TV','TUV'),
      ('Uganda','UG','UGA'),
      ('Ukraine','UA','UKR'),
      ('United Arab Emirates','AE','ARE'),
      ('United Kingdom of Great Britain and Northern Ireland','GB','GBR'),
      ('United States Minor Outlying Islands','UM','UMI'),
      ('United States of America','US','USA'),
      ('Uruguagy','UY','URY'),
      ('Uzbekistan','UZ','UZB'),
      ('Vanuatu','VU','VUT'),
      ('Venezuela, Bolivarian Republic of','VE','VEN'),
      ('Viet Nam','VN','VNM'),
      ('Virgin Islands, British','VG','VGB'),
      ('Virgin Islands, U.S.','VI','VIR'),
      ('Wallis and Futuna','WF','WLF'),
      ('Western Sahara','EH','ESH'),
      ('Yemen','YE','YEM'),
      ('Zambia','ZM','ZMB'),
      ('Zimbabwe','ZW','ZWE')
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()

def sat_stage_fake_table_drop(conn):
    q = "drop table if exists sat_stage_fake"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def sat_stage_fake_table_create(conn):
    q = """
    create table if not exists sat_stage_fake (
    siss_id integer,
    test_id text,
    test_component text,
    test_date,
    data_src text,
    score text,
    date_loaded text,
    last text,
    first text,
    middle text,
    email text,
    address1 text,
    city text,
    state text,
    postal text,
    country text,
    birthdate text,
    homephone text,
    status text,
    created text,
    last_update text,
    satrecord_id text,
    primary key (siss_id, test_id, test_component, test_date))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def sat_stage_fake_seq_table_create(conn):
    q = """
    create table if not exists sat_stage_fake_seq(
    id integer,
    test_value text default '',
    primary key (id ASC))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return
        
def sat_stage_fake_seq_table_drop(conn):
    q = "drop table if exists sat_stage_fake_seq"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return



def act_stage_fake_table_drop(conn):
    q = "drop table if exists act_stage_fake"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def act_stage_fake_table_create(conn):
    q = """
    create table if not exists act_stage_fake (
    siss_id integer,
    test_id text,
    test_component text,
    test_date,
    data_src text,
    score text,
    date_loaded text,
    last text,
    first text,
    middle text,
    email text,
    address1 text,
    city text,
    state text,
    postal text,
    country text,
    birthdate text,
    homephone text,
    cphone text,
    ophone text,
    status text,
    created text,
    last_update text,
    actrecord_id text,
    primary key (siss_id, test_id, test_component, test_date))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def act_stage_fake_seq_table_create(conn):
    q = """
    create table if not exists act_stage_fake_seq(
    id integer,
    test_value text default '',
    primary key (id ASC))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return
        
def act_stage_fake_seq_table_drop(conn):
    q = "drop table if exists act_stage_fake_seq"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def toefl_exclude_table_drop(conn):
    q = "drop table if exists toefl_exclude"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def toefl_exclude_table_create(conn):
    q = """
    create table if not exists toefl_exclude (
    toefl_record_id integer,
    emplid text,
    excl_date text,
    scc_temp_id text,
    primary key (toefl_record_id, emplid))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def toefl_matched_keys_table_drop(conn):
    q = "drop table if exists toefl_matched_keys"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def toefl_matched_keys_table_create(conn):
    q = """
    create table if not exists toefl_matched_keys (
    toefl_record_id integer,
    siss_load_date text,
    emplid text,
    match_date text,
    scc_temp_id text,
    primary key (toefl_record_id, emplid))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def toefl_file_table_drop(conn):
    q = "drop table if exists toefl_file"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return
    
def toefl_file_table_create(conn):
    q = """
    create table if not exists toefl_file(
    toefl_file_id integer,
    filename text,
    loaddate text,
    lines integer,
    primary key(toefl_file_id ASC))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return
    
def toefl_record_table_drop(conn):
    q = "drop table if exists toefl_record"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def toefl_record_table_create(conn):
    q = """
    create table if not exists toefl_record(
    toefl_record_id integer,
    toefl_file_id integer,
    fileline int,
    icode text,
    dcode text,
    ran text,
    last text,
    first text,
    middle text,
    a1 text,
    a2 text,
    a3 text,
    a4 text,
    city text,
    state text,
    country text,
    countryde text,
    postal text,
    nat_country text,
    nat_countryde text,
    nat_lang text,
    nat_langde text,
    dob text,
    gendernum text,
    admin_date text,
    test_ctr_code text,
    test_type text,
    listen_ind text,
    speak_ind text,
    ibt_listening text,
    ibt_reading text,
    ibt_speaking text,
    ibt_writing text,
    ibt_total text,
    pb_reason text,
    pb_degree text,
    pb_sec1 text,
    pb_sec2 text,
    pb_sec3 text,
    pb_conv_twe text,
    pb_total text,
    pb_year text,
    pb_times_taken text,
    pb_nrsp_offt text,
    email text,
    ibt_rpdt_test_ctr_code text,
    test_ctr_country text,
    identification_type text,
    id_number text,
    id_country text,
    rpdt_listening text,
    rpdt_reading text,
    rpdt_writing text,
    dob_ex text,
    gender_ex text,
    mb_ibt_l text,
    mb_ibt_ltd text,
    mb_ibt_r text,
    mb_ibt_rtd text,
    mb_ibt_w text,
    mb_ibt_wtd text,
    mb_ibt_s text,
    mb_ibt_std text,
    mb_ibt_t text,
    mb_ibt_taod text,
    ess_l text,
    ess_r text,
    ess_w text,
    ess_s text,
    ess_tbs text,
    ess_mb_l text,
    ess_mb_ltd text,
    ess_mb_r text,
    ess_mb_rtd text,
    ess_mb_w text,
    ess_mb_wtd text,
    ess_mb_s text,
    ess_mb_std text,
    ess_mb_tbs text,
    ess_mb_aod text,
    ess_cefr_l text,
    ess_cefr_r text,
    ess_cefr_s text,
    ess_cefr_tbs text,
    ess_found_sc text,
    ess_found_vk text,
    ess_cefr_w text,
    canceled_dt text default '',
    digparts text,
    digest_nld text,
    primary key(toefl_record_id ASC),
    FOREIGN KEY (toefl_file_id) REFERENCES toefl_file(toefl_file_id))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def toefl_match_table_drop(conn):
    q = "drop table if exists toefl_match"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def toefl_match_table_create(conn):
    q = """
    create table if not exists toefl_match (
    ID integer,
    toefl_record_id integer,
    categ text,
    scc_temp_id integer,
    emplid text,
    AP_LAST text,
    SUSP_LAST text,
    AP_FIRST text,
    AP_PREF text,
    SUSP_FIRST text,
    AP_MID text,
    SMI text,
    AX text,
    SX text,
    AP_DOB text,
    SUSP_DOB text,
    AP_MA1 text,
    AP_HA1 text,
    AP_SA1 text,
    SUSP_ADDR1 text,
    SUSP_ADDR2 text,
    SUSP_ADDR3 text,
    SUSP_ADDR4 text,
    AP_MCITY text,
    AP_HCITY text,
    AP_SCITY text,
    SUSP_CITY text,
    AP_MSTATE text,
    AP_HSTATE text,
    AP_SSTATE text,
    SUSP_STATE text,
    AP_MPOSTAL text,
    AP_HPOSTAL text,
    SUSP_POSTAL text,
    AP_MCO text,
    AP_HCO text,
    AP_SCO text,
    SUSP_CO text,
    EMAIL text,
    MOEMAIL text,
    FAEMAIL text,
    SUSP_EMAIL text,
    RAT text,
    AE text,
    ADR text,
    DOB text,
    POS text,
    FN text,
    EM text,
    PEM text,
    NPL text,
    ANO text,
    LN text,
    tdate text,
    ttype text,
    ibt_list text,
    ibt_read text,
    ibt_spea text,
    ibt_writ text,
    ibt_tot text,
    pb_sec1 text,
    pb_sec2 text,
    pb_sec3 text,
    pb_conv_twe text,
    pb_total text,
    rpdt_list text,
    rpdt_read text,
    rpdt_writ text,
    ran text,
    filename text,
    line int,
    PRIMARY KEY (ID ASC))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def toefl_stage_table_drop(conn):
    q = "drop table if exists toefl_stage"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def toefl_stage_table_create(conn):
    q = """
    create table if not exists toefl_stage(
    scc_temp_id       integer,
    ran               text,
    last              text,
    first             text,
    middle            text,
    email             text,
    addr1             text,
    city              text,
    state             text,
    postal            text,
    country           text,
    dob               text,
    status            text,
    last_upd          text,
    emplid            text,
    digparts          text,
    digest_nld        text not null unique,
    toefl_record_id   integer,
    query_pass        integer default 0,
    primary key (scc_temp_id)
    )
    """

    q2 = "create index toefl_stage_idx_digest_nld on toefl_stage(digest_nld)"
    q3 = "create index toefl_stage_idx_query_pass on toefl_stage(query_pass)"
    q4 = "create index toefl_stage_idx_ran on toefl_stage(ran)"
    
    cur = conn.cursor()
    cur.execute(q)
    cur.execute(q2)
    cur.execute(q3)
    cur.execute(q4)
    conn.commit()
    return


def slate_country_table_drop(conn):
    q = "drop table if exists slate_country"
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return

def slate_country_table_create(conn):
    q = """
    create table if not exists slate_country (
    country text,
    country3 text,
    primary key(country))
    """
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()
    return


