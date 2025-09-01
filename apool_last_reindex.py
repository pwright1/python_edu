#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved. 

import sys
import traceback
import sqlite3
import os.path
from pw_utils import mydb_utils
from pw_utils import string_utils

note = """
"""

class Bork:
    debug = False
    
    def __init__(self):
        self.script_dir = mydb_utils.get_python_script_dir()
        pass

    def drop(self, conn):
        cur = conn.cursor()
        names_str = "appno city country dob email emplid fi first last middle phone postal pref state"
        names_arr = string_utils.pct_w(names_str)
        for name in names_arr:
            q = f"drop index if exists apool_last_{name}_idx"
            cur.execute(q)
        cur.close()
    
    def reindex(self, conn):
        cur = conn.cursor()
        qindex = """create index apool_last_appno_idx on apool_last(appno)
        create index apool_last_city_idx on apool_last(upper(city))
        create index apool_last_country_idx on apool_last(country)
        create index apool_last_dob_idx on apool_last(dob)
        create index apool_last_email_idx on apool_last(upper(email))
        create index apool_last_emplid_idx on apool_last(emplid)
        create index apool_last_fi_idx on apool_last(upper(substr(first,1,1)))
        create index apool_last_first_idx on apool_last(upper(first))
        create index apool_last_last_idx on apool_last(upper(last))
        create index apool_last_middle_idx on apool_last(upper(middle))
        create index apool_last_phone_idx on apool_last(phone)
        create index apool_last_postal_idx on apool_last(postal)
        create index apool_last_pref_idx on apool_last(upper(pref))
        create index apool_last_state_idx on apool_last(state)
        """
        query_arr = qindex.split("\n")
        for query in query_arr:
            print(query)
            cur.execute(query)
        cur.close()    
        return
        
    def go(self, conn):
        if len(sys.argv[1:]) != 0:
            print("use apool_last_reindex.py")
            sys.exit(-1)

        self.drop(conn)
        self.reindex(conn)
            
        pass # def go
    pass # class Bork
        
def main():
    conn = None
    try:
        pools_db_name = "pools.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        pools_db_file = os.path.join(db_dir, pools_db_name)
        if not os.path.exists(pools_db_file):
            raise RuntimeError(f"pools db file {scores_db_file} not found")

        conn = mydb_utils.sqlite3_connect(pools_db_file)
        b = Bork()
        #b.debug = True
        b.drop(conn)
        b.go(conn)
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass
        
main()

