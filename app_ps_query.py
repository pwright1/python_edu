#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import csv
from pw_utils import mydb_utils
from pw_utils import string_utils

note = """
"""

class Bork:
    debug = False
    
    #--------------------------------------
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        pass

    #--------------------------------------
    def main_q_fields(self):
        field_names="""slate_person_id
        slate_appl_nbr
        emplid
        duid_t
        academic_career
        admit_term
        name_prefix
        name_last
        name_first
        name_first_preferred
        name_middle
        name_suffix
        name_maiden_t
        birth_date
        birth_place
        birth_country
        birth_state
        gender
        marital_status
        national_id
        military_status_t
        citizenship_status_us
        citizenship_country_non_us_1
        citizenship_country_non_us_2
        address_country_home
        address_address1_home
        address_address2_home
        address_address3_home
        address_city_home
        address_county_home
        address_address_state_home
        address_address_postal_home
        address_country_mail
        address_address1_mail
        address_address2_mail
        address_address3_mail
        address_city_mail
        address_address_state_mail
        address_address_postal_mail
        addr_cou_intl_t
        addr_a1_intl_t
        addr_a2_intl_t
        addr_a3_intl_t
        addr_city_intl_t
        addr_state_intl_t
        addr_postal_intl_t
        addr_cou_work_t
        addr_a1_work_t
        addr_a2_work_t
        addr_a3_work_t
        addr_city_work_t
        addr_state_work_t
        addr_postal_work_t
        phone_country_code_home
        phone_number_home
        phone_country_code_cell
        phone_number_cell
        phone_country_code_work
        phone_number_work
        phone_number_extension_work
        phone_number_pref_type
        email_address_other
        email_address_work
        email_address_home
        url_address_skyp
        ethnic_hispanic_latino
        ethnic_grp_codes
        ethnic_grp_addl_dtl
        religious_preference
        du_judicatory_t
        du_district_t
        du_cert_candidate_t
        application_center
        admit_type
        financial_aid_interest
        notification_plan
        application_date
        application_method
        academic_level
        last_school_attended
        appl_fee_status
        appl_fee_amt_t
        appl_fee_paid
        waive_amt
        academic_program
        academic_program_dual_t
        joint_program_approved_t
        action_reason_t
        campus_t
        academic_plan_1
        academic_sub_plan_1_1_t
        academic_sub_plan_1_2_t
        academic_sub_plan_1_3_t
        academic_plan_2_t
        plan_sequence_2_t
        academic_sub_plan_2_1_t
        academic_sub_plan_2_2_t
        academic_sub_plan_2_3_t
        academic_plan_3_t
        plan_sequence_3_t
        academic_sub_plan_3_1_t
        academic_sub_plan_3_2_t
        academic_sub_plan_3_3_t
        recruitment_category_multi
        recruitment_category_01_t
        recruitment_sub_category_01_t
        recruitment_description_01_t
        recruitment_category_02_t
        recruitment_sub_category_02_t
        recruitment_description_02_t
        recruitment_category_03_t
        recruitment_sub_category_03_t
        recruitment_description_03_t
        recruitment_category_04_t
        recruitment_sub_category_04_t
        recruitment_description_04_t
        recruitment_category_05_t
        recruitment_sub_category_05_t
        recruitment_description_05_t
        recruitment_category_06_t
        recruitment_sub_category_06_t
        recruitment_description_06_t
        recruitment_category_07_t
        recruitment_sub_category_07_t
        recruitment_description_07_t
        recruitment_category_08_t
        recruitment_sub_category_08_t
        recruitment_description_08_t
        recruitment_category_09_t
        recruitment_sub_category_09_t
        recruitment_description_09_t
        recruitment_category_10_t
        recruitment_sub_category_10_t
        recruitment_description_10_t
        academic_interest_1
        academic_interest_2
        academic_interest_3"""
        
        field_names_arr = string_utils.pct_w(field_names)
        return field_names_arr
        
    #--------------------------------------
    def rel_q_fields(self):
        field_names="""rltnshp_1_people_relation
        rltnshp_1_name
        rltnshp_1_highest_educ_level
        rltnshp_1_employer
        rltnshp_1_marital_status
        rltnshp_1_name_prefix
        rltnshp_1_phone_day
        rltnshp_1_phone_eve_t
        rltnshp_1_email
        rltnshp_1_sex_t
        rltnshp_1_comments_t
        rltnshp_1_birth_country
        rltnshp_1_du_rel_deceased
        rltnshp_1_title
        rltnshp_1_inst_affiliation_1_alum
        rltnshp_1_institution_1_t
        rltnshp_1_to_dt_1_t
        rltnshp_1_inst_affiliation_2_employee
        rltnshp_1_institution_2_t
        rltnshp_1_to_dt_2_t
        rltnshp_2_people_relation
        rltnshp_2_name
        rltnshp_2_highest_educ_level
        rltnshp_2_employer
        rltnshp_2_marital_status
        rltnshp_2_name_prefix
        rltnshp_2_phone_day
        rltnshp_2_phone_eve_t
        rltnshp_2_email
        rltnshp_2_sex_t
        rltnshp_2_comments_t
        rltnshp_2_birth_country
        rltnshp_2_du_rel_deceased
        rltnshp_2_title
        rltnshp_2_inst_affiliation_1_alum
        rltnshp_2_institution_1_t
        rltnshp_2_to_dt_1_t
        rltnshp_2_inst_affiliation_2_employee
        rltnshp_2_institution_2_t
        rltnshp_2_to_dt_2_t
        rltnshp_3_people_relation
        rltnshp_3_name
        rltnshp_3_highest_educ_level
        rltnshp_3_employer
        rltnshp_3_marital_status
        rltnshp_3_name_prefix
        rltnshp_3_phone_day
        rltnshp_3_phone_eve_t
        rltnshp_3_email
        rltnshp_3_sex_t
        rltnshp_3_comments_t
        rltnshp_3_birth_country
        rltnshp_3_du_rel_deceased
        rltnshp_3_title
        rltnshp_3_inst_affiliation_1_alum
        rltnshp_3_institution_1_t
        rltnshp_3_to_dt_1_t
        rltnshp_3_inst_affiliation_2_employee
        rltnshp_3_institution_2_t
        rltnshp_3_to_dt_2_t
        rltnshp_4_people_relation
        rltnshp_4_name
        rltnshp_4_highest_educ_level
        rltnshp_4_employer
        rltnshp_4_marital_status
        rltnshp_4_name_prefix
        rltnshp_4_phone_day
        rltnshp_4_phone_eve_t
        rltnshp_4_email
        rltnshp_4_sex_t
        rltnshp_4_comments_t
        rltnshp_4_birth_country
        rltnshp_4_du_rel_deceased
        rltnshp_4_title
        rltnshp_4_inst_affiliation_1_alum
        rltnshp_4_institution_1_t
        rltnshp_4_to_dt_1_t
        rltnshp_4_inst_affiliation_2_employee
        rltnshp_4_institution_2_t
        rltnshp_4_to_dt_2_t
        rltnshp_5_people_relation
        rltnshp_5_name
        rltnshp_5_highest_educ_level
        rltnshp_5_employer
        rltnshp_5_marital_status
        rltnshp_5_name_prefix
        rltnshp_5_phone_day
        rltnshp_5_phone_eve_t
        rltnshp_5_email
        rltnshp_5_sex_t
        rltnshp_5_comments_t
        rltnshp_5_birth_country
        rltnshp_5_du_rel_deceased
        rltnshp_5_title
        rltnshp_5_inst_affiliation_1_alum
        rltnshp_5_institution_1_t
        rltnshp_5_to_dt_1_t
        rltnshp_5_inst_affiliation_2_employee
        rltnshp_5_institution_2_t
        rltnshp_5_to_dt_2_t
        rltnshp_6_people_relation
        rltnshp_6_name
        rltnshp_6_highest_educ_level
        rltnshp_6_employer
        rltnshp_6_marital_status
        rltnshp_6_name_prefix
        rltnshp_6_phone_day
        rltnshp_6_phone_eve_t
        rltnshp_6_email
        rltnshp_6_sex_t
        rltnshp_6_comments_t
        rltnshp_6_birth_country
        rltnshp_6_du_rel_deceased
        rltnshp_6_title
        rltnshp_6_inst_affiliation_1_alum
        rltnshp_6_institution_1_t
        rltnshp_6_to_dt_1_t
        rltnshp_6_inst_affiliation_2_employee
        rltnshp_6_institution_2_t
        rltnshp_6_to_dt_2_t
        rltnshp_7_people_relation
        rltnshp_7_name
        rltnshp_7_highest_educ_level
        rltnshp_7_employer
        rltnshp_7_marital_status
        rltnshp_7_name_prefix
        rltnshp_7_phone_day
        rltnshp_7_phone_eve_t
        rltnshp_7_email
        rltnshp_7_sex_t
        rltnshp_7_comments_t
        rltnshp_7_birth_country
        rltnshp_7_du_rel_deceased
        rltnshp_7_title
        rltnshp_7_inst_affiliation_1_alum
        rltnshp_7_institution_1_t
        rltnshp_7_to_dt_1_t
        rltnshp_7_inst_affiliation_2_employee
        rltnshp_7_institution_2_t
        rltnshp_7_to_dt_2_t
        rltnshp_8_people_relation
        rltnshp_8_name
        rltnshp_8_highest_educ_level
        rltnshp_8_employer
        rltnshp_8_marital_status
        rltnshp_8_name_prefix
        rltnshp_8_phone_day
        rltnshp_8_phone_eve_t
        rltnshp_8_email
        rltnshp_8_sex_t
        rltnshp_8_comments_t
        rltnshp_8_birth_country
        rltnshp_8_du_rel_deceased
        rltnshp_8_title
        rltnshp_8_inst_affiliation_1_alum
        rltnshp_8_institution_1_t
        rltnshp_8_to_dt_1_t
        rltnshp_8_inst_affiliation_2_employee
        rltnshp_8_institution_2_t
        rltnshp_8_to_dt_2_t"""

        field_names_arr = string_utils.pct_w(field_names)
        return field_names_arr
        
    #--------------------------------------
    def edu_q_fields(self):
        field_names="""educ_1_ext_org_id
        educ_1_ext_career
        educ_1_ls_data_source
        educ_1_transcript_flag
        educ_1_transcript_type
        educ_1_transcript_status
        educ_1_ext_acad_level
        educ_1_transcript_date
        educ_1_ext_term_type_t
        educ_1_term_year_t
        educ_1_received_dt_t
        educ_1_from_dt_t
        educ_1_to_dt_t
        educ_1_ext_summ_type
        educ_1_unit_type_t
        educ_1_unt_atmp_total_t
        educ_1_unt_comp_total_t
        educ_1_class_rank_t
        educ_1_class_size_t
        educ_1_gpa_type_t
        educ_1_ext_gpa_t
        educ_1_convert_gpa_t
        educ_1_percentile_t
        educ_1_rank_type_t
        educ_1_comments_1_1_t
        educ_1_comments_1_2_t
        educ_1_degree_t
        educ_1_descr_t
        educ_1_degree_dt_t
        educ_1_degree_status_t
        educ_1_honors_category_t
        educ_1_ext_subj_area_1_1_t
        educ_1_ext_subj_area_1_2_t
        educ_1_field_of_study_1_1_t
        educ_1_field_of_study_1_2_t
        educ_2_ext_org_id
        educ_2_ext_career
        educ_2_ls_data_source
        educ_2_transcript_flag
        educ_2_transcript_type
        educ_2_transcript_status
        educ_2_ext_acad_level
        educ_2_transcript_date
        educ_2_ext_term_type_t
        educ_2_term_year_t
        educ_2_received_dt_t
        educ_2_from_dt_t
        educ_2_to_dt_t
        educ_2_ext_summ_type
        educ_2_unit_type_t
        educ_2_unt_atmp_total_t
        educ_2_unt_comp_total_t
        educ_2_class_rank_t
        educ_2_class_size_t
        educ_2_gpa_type_t
        educ_2_ext_gpa_t
        educ_2_convert_gpa_t
        educ_2_percentile_t
        educ_2_rank_type_t
        educ_2_comments_1_1_t
        educ_2_comments_1_2_t
        educ_2_degree_t
        educ_2_descr_t
        educ_2_degree_dt_t
        educ_2_degree_status_t
        educ_2_honors_category_t
        educ_2_ext_subj_area_1_1_t
        educ_2_ext_subj_area_1_2_t
        educ_2_field_of_study_1_1_t
        educ_2_field_of_study_1_2_t
        educ_3_ext_org_id
        educ_3_ext_career
        educ_3_ls_data_source
        educ_3_transcript_flag
        educ_3_transcript_type
        educ_3_transcript_status
        educ_3_ext_acad_level
        educ_3_transcript_date
        educ_3_ext_term_type_t
        educ_3_term_year_t
        educ_3_received_dt_t
        educ_3_from_dt_t
        educ_3_to_dt_t
        educ_3_ext_summ_type
        educ_3_unit_type_t
        educ_3_unt_atmp_total_t
        educ_3_unt_comp_total_t
        educ_3_class_rank_t
        educ_3_class_size_t
        educ_3_gpa_type_t
        educ_3_ext_gpa_t
        educ_3_convert_gpa_t
        educ_3_percentile_t
        educ_3_rank_type_t
        educ_3_comments_1_1_t
        educ_3_comments_1_2_t
        educ_3_degree_t
        educ_3_descr_t
        educ_3_degree_dt_t
        educ_3_degree_status_t
        educ_3_honors_category_t
        educ_3_ext_subj_area_1_1_t
        educ_3_ext_subj_area_1_2_t
        educ_3_field_of_study_1_1_t
        educ_3_field_of_study_1_2_t
        educ_4_ext_org_id
        educ_4_ext_career
        educ_4_ls_data_source
        educ_4_transcript_flag
        educ_4_transcript_type
        educ_4_transcript_status
        educ_4_ext_acad_level
        educ_4_transcript_date
        educ_4_ext_term_type_t
        educ_4_term_year_t
        educ_4_received_dt_t
        educ_4_from_dt_t
        educ_4_to_dt_t
        educ_4_ext_summ_type
        educ_4_unit_type_t
        educ_4_unt_atmp_total_t
        educ_4_unt_comp_total_t
        educ_4_class_rank_t
        educ_4_class_size_t
        educ_4_gpa_type_t
        educ_4_ext_gpa_t
        educ_4_convert_gpa_t
        educ_4_percentile_t
        educ_4_rank_type_t
        educ_4_comments_1_1_t
        educ_4_comments_1_2_t
        educ_4_degree_t
        educ_4_descr_t
        educ_4_degree_dt_t
        educ_4_degree_status_t
        educ_4_honors_category_t
        educ_4_ext_subj_area_1_1_t
        educ_4_ext_subj_area_1_2_t
        educ_4_field_of_study_1_1_t
        educ_4_field_of_study_1_2_t
        educ_5_ext_org_id
        educ_5_ext_career
        educ_5_ls_data_source
        educ_5_transcript_flag
        educ_5_transcript_type
        educ_5_transcript_status
        educ_5_ext_acad_level
        educ_5_transcript_date
        educ_5_ext_term_type_t
        educ_5_term_year_t
        educ_5_received_dt_t
        educ_5_from_dt_t
        educ_5_to_dt_t
        educ_5_ext_summ_type
        educ_5_unit_type_t
        educ_5_unt_atmp_total_t
        educ_5_unt_comp_total_t
        educ_5_class_rank_t
        educ_5_class_size_t
        educ_5_gpa_type_t
        educ_5_ext_gpa_t
        educ_5_convert_gpa_t
        educ_5_percentile_t
        educ_5_rank_type_t
        educ_5_comments_1_1_t
        educ_5_comments_1_2_t
        educ_5_degree_t
        educ_5_descr_t
        educ_5_degree_dt_t
        educ_5_degree_status_t
        educ_5_honors_category_t
        educ_5_ext_subj_area_1_1_t
        educ_5_ext_subj_area_1_2_t
        educ_5_field_of_study_1_1_t
        educ_5_field_of_study_1_2_t
        educ_6_ext_org_id
        educ_6_ext_career
        educ_6_ls_data_source
        educ_6_transcript_flag
        educ_6_transcript_type
        educ_6_transcript_status
        educ_6_ext_acad_level
        educ_6_transcript_date
        educ_6_ext_term_type_t
        educ_6_term_year_t
        educ_6_received_dt_t
        educ_6_from_dt_t
        educ_6_to_dt_t
        educ_6_ext_summ_type
        educ_6_unit_type_t
        educ_6_unt_atmp_total_t
        educ_6_unt_comp_total_t
        educ_6_class_rank_t
        educ_6_class_size_t
        educ_6_gpa_type_t
        educ_6_ext_gpa_t
        educ_6_convert_gpa_t
        educ_6_percentile_t
        educ_6_rank_type_t
        educ_6_comments_1_1_t
        educ_6_comments_1_2_t
        educ_6_degree_t
        educ_6_descr_t
        educ_6_degree_dt_t
        educ_6_degree_status_t
        educ_6_honors_category_t
        educ_6_ext_subj_area_1_1_t
        educ_6_ext_subj_area_1_2_t
        educ_6_field_of_study_1_1_t
        educ_6_field_of_study_1_2_t
        accomplishment_lic_cert_1_t"""

        field_names_arr = string_utils.pct_w(field_names)
        return field_names_arr
    
    #--------------------------------------
    def work_q_fields(self):
        field_names="""work_1_employment_descr_t
        work_1_city_t
        work_1_country_t
        work_1_state_t
        work_1_phone_
        work_1_start_dt_t
        work_1_end_dt_t
        work_1_title_long_t
        work_1_ending_rate_t
        work_1_pay_freq_abbrev_t
        work_1_us_sic_cd_t
        work_1_descrlong_t
        work_2_employment_descr_t
        work_2_city_t
        work_2_country_t
        work_2_state_t
        work_2_phone_
        work_2_start_dt_t
        work_2_end_dt_t
        work_2_title_long_t
        work_2_ending_rate_t
        work_2_pay_freq_abbrev_t
        work_2_us_sic_cd_t
        work_2_descrlong_t
        work_3_employment_descr_t
        work_3_city_t
        work_3_country_t
        work_3_state_t
        work_3_phone_
        work_3_start_dt_t
        work_3_end_dt_t
        work_3_title_long_t
        work_3_ending_rate_t
        work_3_pay_freq_abbrev_t
        work_3_us_sic_cd_t
        work_3_descrlong_t
        work_4_employment_descr_t
        work_4_city_t
        work_4_country_t
        work_4_state_t
        work_4_phone_
        work_4_start_dt_t
        work_4_end_dt_t
        work_4_title_long_t
        work_4_ending_rate_t
        work_4_pay_freq_abbrev_t
        work_4_us_sic_cd_t
        work_4_descrlong_t
        work_5_employment_descr_t
        work_5_city_t
        work_5_country_t
        work_5_state_t
        work_5_phone_
        work_5_start_dt_t
        work_5_end_dt_t
        work_5_title_long_t
        work_5_ending_rate_t
        work_5_pay_freq_abbrev_t
        work_5_us_sic_cd_t
        work_5_descrlong_t
        work_6_employment_descr_t
        work_6_city_t
        work_6_country_t
        work_6_state_t
        work_6_phone_
        work_6_start_dt_t
        work_6_end_dt_t
        work_6_title_long_t
        work_6_ending_rate_t
        work_6_pay_freq_abbrev_t
        work_6_us_sic_cd_t
        work_6_descrlong_t
        work_7_employment_descr_t
        work_7_city_t
        work_7_country_t
        work_7_state_t
        work_7_phone_
        work_7_start_dt_t
        work_7_end_dt_t
        work_7_title_long_t
        work_7_ending_rate_t
        work_7_pay_freq_abbrev_t
        work_7_us_sic_cd_t
        work_7_descrlong_t
        work_8_employment_descr_t
        work_8_city_t
        work_8_country_t
        work_8_state_t
        work_8_phone_
        work_8_start_dt_t
        work_8_end_dt_t
        work_8_title_long_t
        work_8_ending_rate_t
        work_8_pay_freq_abbrev_t
        work_8_us_sic_cd_t
        work_8_descrlong_t
        work_9_employment_descr_t
        work_9_city_t
        work_9_country_t
        work_9_state_t
        work_9_phone_
        work_9_start_dt_t
        work_9_end_dt_t
        work_9_title_long_t
        work_9_ending_rate_t
        work_9_pay_freq_abbrev_t
        work_9_us_sic_cd_t
        work_9_descrlong_t
        work_time_unit_1_t
        work_time_unit_2_t
        vendor_id"""
        
        field_names_arr = string_utils.pct_w(field_names)
        return field_names_arr
        
    #--------------------------------------
    def lang_q_fields(self):
        field_names="""visa_permit_type_t
        visa_wrkpmt_status_t
        lang_1_lang_cd
        lang_1_native_lang
        lang_1_speak_prof
        lang_1_read_prof
        lang_1_write_prof
        lang_2_lang_cd
        lang_2_native_lang
        lang_2_speak_prof
        lang_2_read_prof
        lang_2_write_prof
        lang_3_lang_cd
        lang_3_native_lang
        lang_3_speak_prof
        lang_3_read_prof
        lang_3_write_prof
        lang_4_lang_cd
        lang_4_native_lang
        lang_4_speak_prof
        lang_4_read_prof
        lang_4_write_prof
        lang_5_lang_cd
        lang_5_native_lang
        lang_5_speak_prof
        lang_5_read_prof
        lang_5_write_prof"""

        field_names_arr = string_utils.pct_w(field_names)
        return field_names_arr
        
    #--------------------------------------
    def update_loaddate(self, conn, tup):
        update_q = """
        update app_ps_main set loaddate = ? where id = ?
        """
        cur = conn.cursor()
        res = cur.execute(update_q, tup)
        pass

    #--------------------------------------
    def go(self, conn):
        if len(sys.argv[1:]) != 0:
            print("use app_ps_query.py")
            sys.exit(-1)

        loaddate = mydb_utils.get_db_ts()    
        main_q_arr = self.main_q_fields()
        rel_q_arr = self.rel_q_fields()
        edu_q_arr = self.edu_q_fields()
        work_q_arr = self.work_q_fields()
        lang_q_arr = self.lang_q_fields()

        select_q = "select m.id "
        for field in main_q_arr:
            select_q += f", m.{field}"
        for field in rel_q_arr:
            select_q += f", r.{field}"
        for field in edu_q_arr:
            select_q += f", e.{field}"
        for field in work_q_arr:
            select_q += f", w.{field}"
        for field in lang_q_arr:
            select_q += f", l.{field}"

        addl_q = """
        from app_ps_main m, app_ps_rel r, app_ps_edu e, app_ps_work w, app_ps_lang l
        where m.id = r.id
        and m.id = e.id
        and m.id = w.id
        and m.id = l.id
        and m.omit = ''            -- for testing
        and m.appno = ''           -- set the app number so it won't load an existing app found
        and m.loaddate = ''        -- not query the same data twice
        and m.ps_update_date = ''  -- lets you know you ran the ps emplid update script
                                   -- will be blank for ones you manually updated the appno and not load
        order by m.id
        """
        select_q += addl_q
        #print(select_q)
        
        ts = mydb_utils.get_file_ts()
        out_filename = f"ps_loadfile_{ts}.txt"
        fout = open(out_filename, "w", encoding="UTF-8")
        
        update_ids = []
        cur = conn.cursor()
        count = 0
        for row in cur.execute(select_q):
            id = row[0]
            mydb_utils.uga_out_pipe(fout, row[1:])
            update_ids.append(f"{id}^{loaddate}")
            count += 1
        fout.close()
        cur.close()

        print(f"{count} query rows returned")
        if count == 0:
            try:
                os.remove(out_filename)
            except Exception as e:
                print(f"exception on file delete {e}")
        else:
            for i, id_date in enumerate(update_ids):
                did, date = id_date.split("^")
                # disabled, be sure not to load the same data twice
                #self.update_loaddate(conn, (date, did))
                if i % 20 == 0:
                    conn.commit()
            conn.commit()
        pass # def go
    pass # class
        
def main():
    conn = None
    try:
        apps_db_name = "apps.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        apps_db_file = os.path.join(db_dir, apps_db_name)
        if not os.path.exists(apps_db_file):
            raise RuntimeError(f"apps db file {apps_db_file} not found")

        conn = mydb_utils.sqlite3_connect(apps_db_file)
        b = Bork()
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()
