#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
import csv
import time
from pw_utils import mydb_utils
from pw_utils import string_utils
import re

note = """
"""

class OrgEntry:
    def __init__(self, orgid, estatus):
        self.orgid = orgid
        self.estatus = estatus

class Bork:
    debug = False
    
    #--------------------------------------
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        self.org_hash = {}
        
    #--------------------------------------
    def read_org_table(self, conn):
        q = "select orgid, estatus from org"
        cur = conn.cursor()
        print("reading org table")
        i = 0
        for row in cur.execute(q):
            orgid, estatus = row
            org_entry = self.org_hash.get(orgid, None)
            if org_entry == None:
                org_entry = OrgEntry(orgid, estatus)
                self.org_hash[orgid] = org_entry
            if i % 10000 == 0:
                print(i)
            i += 1

    #--------------------------------------
    def valid_org(self,orgid):
        exists, active = [False, False]
        org_entry = self.org_hash.get(orgid, None)
        if org_entry == None:
            exists = False
            active = False
            return [exists, active]
        else:
            exists = True
            active = False
            if org_entry.estatus == "A":
                active = True
            return [exists, active]
    
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

        all_field_names = []
        all_table_names = []
        all_field_names.append("id")
        all_table_names.append("app_ps_main")
        
        select_q = "select m.id "
        for field in main_q_arr:
            select_q += f", m.{field}"
            all_table_names.append("app_ps_main")
            all_field_names.append(field)
        for field in rel_q_arr:
            select_q += f", r.{field}"
            all_table_names.append("app_ps_rel")
            all_field_names.append(field)
        for field in edu_q_arr:
            select_q += f", e.{field}"
            all_table_names.append("app_ps_edu")
            all_field_names.append(field)
        for field in work_q_arr:
            select_q += f", w.{field}"
            all_table_names.append("app_ps_work")
            all_field_names.append(field)
        for field in lang_q_arr:
            select_q += f", l.{field}"
            all_table_names.append("app_ps_lang")
            all_field_names.append(field)

        addl_q = """
        from app_ps_main m, app_ps_rel r, app_ps_edu e, app_ps_work w, app_ps_lang l
        where m.id = r.id
        and m.id = e.id
        and m.id = w.id
        and m.id = l.id
        and m.omit = ''
        and m.appno = ''
        and m.loaddate = ''
        and m.ps_update_date = ''
        order by m.id
        """
        select_q += addl_q
        #print(select_q)
        
        ts = mydb_utils.get_file_ts()
        out_filename = f"check_{ts}.csv"
        with open(out_filename, "w", encoding="UTF-8") as fout:
            hdr = ["db_id","vendor_id","error", "bad_val", "fixed_val", "update? => y"]
            mydb_utils.uga_out(fout, hdr)
        
            cur = conn.cursor()
            count = 0
            for row in cur.execute(select_q):
                fields = row

                table_hash, row_hash = self.store_fields_in_hash(all_table_names, all_field_names, fields)
                #print(f"{row_hash['vendor_id']}")
                self.do_error_check(table_hash, row_hash, fout)
                count += 1
            cur.close()
        time.sleep(2)
        mydb_utils.csv_to_xlsx(out_filename)
        print("done")
        pass # def go

    #----------------------------
    def store_fields_in_hash(self,all_table_names, all_field_names, fields):
        row_hash = {}
        table_hash = {}
        all_len = len(all_field_names)
        fields_len = len(fields)
        #print(f"all_len {all_len} fields_len {fields_len}")
        for i, field_name in enumerate(all_field_names):
            row_hash[field_name] = fields[i]
            table_hash[field_name] = all_table_names[i]
        return table_hash, row_hash

    #----------------------------
    def do_error_check(self, table_hash, row_hash, fout):
        error_arr = []
        id = row_hash['id']
        vendor_id = row_hash['vendor_id']
        
        # is the slate id blank?
        if row_hash['slate_person_id'] == "":
            table = table_hash['slate_person_id']
            error_arr.append(f"{id}^{vendor_id}^blank {table}.slate_person_id^^^")
        # is the slate app number blank?
        if row_hash['slate_appl_nbr'] == "":
            table = table_hash['slate_appl_nbr']
            error_arr.append(f"{id}^{vendor_id}^blank {table}.slate_appl_nbr^^^")

        # is acad career UGRD?
        if row_hash['academic_career'] != "UGRD":
            table = table_hash['academic_career']
            error_arr.append(f"{id}^{vendor_id}^bad {table}.academic_career^{row_hash['academic_career']}^^")
        # is the admit term 4 digits?
        admit_term = row_hash['admit_term']
        if not re.fullmatch(r"\d{4}",admit_term):
            table = table_hash['admit_term']
            error_arr.append(f"{id}^{vendor_id}^bad {table}.admit_term^{admit_term}^^")

        # is the birth date 8 digits?
        birth_date = row_hash['birth_date']
        if not re.fullmatch(r"\d{8}",birth_date):
            table = table_hash['birth_date']
            error_arr.append(f"{id}^{vendor_id}^bad or blank {table}.birth_date^{birth_date}^^")

        # is the birth country 3 capital letters or blank?
        birth_country = row_hash['birth_country']
        if not (re.fullmatch(r"[A-Z]{3}", birth_country) or birth_country == ""):
            table = table_hash['birth_country']
            error_arr.append(f"{id}^{vendor_id}^bad or blank {table}.birth_country^{birth_country}^^")
        # gender is a required field. is it one of the letters below?
        gender = row_hash['gender']
        if not gender in ["F","M","U","X"]:
            table = table_hash['gender']
            error_arr.append(f"{id}^{vendor_id}^bad or blank {table}.gender^{gender}^^")
        
        marital_status = row_hash['marital_status']
        if len(marital_status) != 1:
            table = table_hash['marital_status']
            error_arr.append(f"{id}^{vendor_id}^bad or blank {table}.marital_status^{marital_status}^^")
        # is the ssn 9 digits (no dashes) or blank?
        national_id = row_hash['national_id']
        if not (re.fullmatch(r"\d{9}",national_id) or national_id == ""):
            table = table_hash['national_id']
            error_arr.append(f"{id}^{vendor_id}^bad {table}.national_id value^{national_id}^^")

        citizenship_status_us = row_hash['citizenship_status_us']
        if citizenship_status_us not in ["1","3","4"]:
            table = table_hash['citizenship_status_us']
            error_arr.append(f"{id}^{vendor_id}^bad {table}.citizenship_status_us value^{citizenship_status_us}^^")
        
        ethnic_hispanic_latino = row_hash['ethnic_hispanic_latino']
        if ethnic_hispanic_latino not in ["Y","N",""]:
            table = table_hash['ethnic_hispanic_latino']
            error_arr.append(f"{id}^{vendor_id}^bad {table}.ethnic_hispanic_latino value^{ethnic_hispanic_latino}^^")

        ethnic_grp_codes = row_hash['ethnic_grp_codes']
        if string_utils.dup_csv_value(ethnic_grp_codes):
            table = table_hash['ethnic_grp_codes']
            error_arr.append(f"{id}^{vendor_id}^dup csv value {table}.ethnic_grp_codes value^{ethnic_grp_codes}^^")

        ethnic_grp_addl_dtl = row_hash['ethnic_grp_addl_dtl']
        if string_utils.dup_csv_value(ethnic_grp_addl_dtl):
            table = table_hash['ethnic_grp_addl_dtl']
            error_arr.append(f"{id}^{vendor_id}^dup csv value {table}.ethnic_grp_addl_dtl value^{ethnic_grp_addl_dtl}^^")

        code_hash = {"": 0,
                     "A&SU": 1,
                     "E-UGU": 2,
                     "TRIN": 1,
                     "ENGR": 2,
                     "UNDECLARED": 1,
                     "N/A": 2,
                     }
            
        academic_program = row_hash['academic_program']
        academic_program_code = code_hash.get(academic_program, 0)

        application_center = row_hash['application_center']
        application_center_code = code_hash.get(application_center, 0)

        academic_plan_1 = row_hash['academic_plan_1']
        academic_plan_1_code = code_hash.get(academic_plan_1, 0)
        
        table = table_hash['academic_program']
        if academic_program not in ["A&SU","E-UGU"]:
            error_arr.append(f"{id}^{vendor_id}^Bad or blank {table}.academic_program value should be E-UGU or A&SU^{academic_program}^^")

        if application_center not in ["TRIN","ENGR"]:
            error_arr.append(f"{id}^{vendor_id}^Bad or blank {table}.application_center value should be ENGR or TRIN^{application_center}^^")

        if academic_plan_1 not in ["UNDECLARED","N/A"]:
            error_arr.append(f"{id}^{vendor_id}^Bad or blank {table}.academic_plan_1 value should be N/A or UNDECLARED^{academic_plan_1}^^")
        
        if academic_program_code > 0 and application_center_code > 0 and academic_plan_1_code > 0:
            if not (academic_program_code == application_center_code and application_center_code == academic_plan_1_code):
                error_arr.append(f"{id}^{vendor_id}^Mismatch between {academic_program}, {application_center}, {academic_plan_1}^^^")
            
        admit_type = row_hash['admit_type']
        if admit_type not in ["FYR","TRF"]:
            table = table_hash['admit_type']
            error_arr.append(f"{id}^{vendor_id}^bad {table}.admit_type value^{admit_type}^^")

            
        financial_aid_interest = row_hash['financial_aid_interest']
        if financial_aid_interest.upper() not in ("Y","N"):
            table = table_hash['financial_aid_interest']
            error_arr.append(f"{id}^{vendor_id}^bad {table}.financial_aid_interest^{financial_aid_interest}^^")

        notification_plan = row_hash['notification_plan']
        if notification_plan not in ["ERLY","REG"]:
            table = table_hash['notification_plan']
            error_arr.append(f"{id}^{vendor_id}^bad {table}.notification_plan^{notification_plan}^^")
        
        last_school_attended = row_hash['last_school_attended']
        if len(last_school_attended) > 0:
            exists, active = self.valid_org(last_school_attended)
            if not exists or not active:
                table = table_hash['last_school_attended']
                error_arr.append(f"{id}^{vendor_id}^inactive or nonexistant org code {table}.last_school_attended^{last_school_attended}^^")

        recruitment_category_multi = row_hash['recruitment_category_multi']
        if string_utils.dup_csv_value(recruitment_category_multi):
            table = table_hash['recruitment_category_multi']
            error_arr.append(f"{id}^{vendor_id}^Duplicate recruitment category {table}.recruitment+category_multi^{recruitment_category_multi}^^")

        academic_1 = row_hash['academic_interest_1']
        academic_2 = row_hash['academic_interest_2']
        academic_3 = row_hash['academic_interest_3']

        if academic_1 == "":
            table = table_hash['academic_interest_1']
            error_arr.append(f"{id}^{vendor_id}^blank academic_interest_1 {table}.academic_interest_1^^^")
        if academic_1 == "":
            table = table_hash['academic_interest_2']
            error_arr.append(f"{id}^{vendor_id}^blank academic_interest_2 {table}.academic_interest_2^^^")
        if academic_1 == "":
            table = table_hash['academic_interest_3']
            error_arr.append(f"{id}^{vendor_id}^blank academic_interest_3 {table}.academic_interest_3^^^")

            
        lang_hash = {}
        lang_1_lang_cd = row_hash['lang_1_lang_cd']
        lang_2_lang_cd = row_hash['lang_2_lang_cd']
        lang_3_lang_cd = row_hash['lang_3_lang_cd']
        lang_4_lang_cd = row_hash['lang_4_lang_cd']
        lang_5_lang_cd = row_hash['lang_5_lang_cd']

        for i, lang in enumerate([lang_1_lang_cd, lang_2_lang_cd, lang_3_lang_cd,
                                  lang_4_lang_cd, lang_5_lang_cd]):
            if lang != "":
                existing_value = lang_hash.get(lang, None)
                if existing_value == None:
                    lang_hash[lang] = i+1
                else:
                    table = table_hash['lang_1_lang_cd']
                    error_arr.append(f"{id}^{vendor_id}^Duplicate language code {table}.lang_{i+1}_lang_cd: {lang} matches {table}.lang_{existing_value}_lang_cd^^^")
        
        # write to a file. provide a header, and a way to reimport the file for fixes
        for err in error_arr:
            id, vendor_id, msg, value, corr_value, update = err.split("^")
            mydb_utils.uga_out(sys.stdout, [id, vendor_id, msg, value, corr_value, update])
            mydb_utils.uga_out(fout, [id, vendor_id, msg, value, corr_value, update])
        pass
    
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
        b.read_org_table(conn)
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()
