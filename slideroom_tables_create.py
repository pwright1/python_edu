#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved.

import sys
import traceback
import sqlite3
import os.path
import time
from datetime import datetime
from pw_utils import mydb_utils
from pw_utils import score_tables_utils as stu


class Bork:
    debug = False
    
    def __init__(self):
        pass

    def drop(self, conn):
        q1 = """
        drop table if exists slideroom_reviewers
        """
        q2 = """
        drop table if exists slideroom_records
        """
        cur = conn.cursor()
        
        for q in [q1,q2]:
            cur.execute(q)
        
    def create(self, conn):
        q1 = """
        create table if not exists slideroom_records(
        id text,
        last text,
        first text,
        ceeb text,
        email text,
        alt_email text default '',
        program text,
        date_submitted text,
        music_other text,
        voice_part text,
        vendor_id text,
        vendor text,
        emplid text default '',
        appno text default '',
        slate_person_id text default '',
        slate_app_no text default '',
        processed text default '',
        matched text default '',
        abbrev_prog text,
        text robot default '',
        primary key (id))
        """

        q2 = """
        create table if not exists slideroom_reviewers(
        id integer,
        overall_rating text,
        reviewer_first text,
        reviewer_last text,
        reviewer_email text,
        reviewer_note text,
        reviewer_combined_rating text,
        date_evaluated text,
        slideroom_record_id text,
        primary key(id asc),
        foreign key(slideroom_record_id) references slideroom_records(id) on delete cascade)
        """

        cur = conn.cursor()
        for q in [q1,q2]:
            cur.execute(q)
        
    def go(self, conn):
        print("this will destroy all local SLIDEROOM data and recreate all tables, are you sure??? (type YES)")
        answer = input()
        if answer == "YES":
            self.drop(conn)
            self.create(conn)
            pass
        pass # def go
    pass # class Bork
        
def main():
    conn = None
    try:
        slideroom_db_name = "slideroom.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        slideroom_db_file = os.path.join(db_dir, slideroom_db_name)
        if not os.path.exists(slideroom_db_file):
            raise RuntimeError(f"slideroom db file {slideroom_db_file} not found")

        conn = mydb_utils.sqlite3_connect(slideroom_db_file)
        b = Bork()
        #b.debug = True
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()

