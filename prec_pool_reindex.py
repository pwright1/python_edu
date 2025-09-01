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
        names_str = "city country dob email emplid first last middle phone postal pref state"
        names_arr = string_utils.pct_w(names_str)
        for name in names_arr:
            q = f"drop index if exists prec_pool_{name}_idx"
            cur.execute(q)
        cur.close()
    
    def reindex(self, conn):
        cur = conn.cursor()
        qindex = """create index prec_pool_city_idx on prec_pool(upper(city))
        create index prec_pool_country_idx on prec_pool(country)
        create index prec_pool_dob_idx on prec_pool(dob)
        create index prec_pool_email_idx on prec_pool(upper(email))
        create index prec_pool_emplid_idx on prec_pool(emplid)
        create index prec_pool_fi_idx on prec_pool(upper(substr(first,1,1)))
        create index prec_pool_first_idx on prec_pool(upper(first))
        create index prec_pool_last_idx on prec_pool(upper(last))
        create index prec_pool_middle_idx on prec_pool(upper(middle))
        create index prec_pool_phone_idx on prec_pool(phone)
        create index prec_pool_postal_idx on prec_pool(postal)
        create index prec_pool_pref_idx on prec_pool(upper(pref))
        create index prec_pool_state_idx on prec_pool(state)
        """
        query_arr = qindex.split("\n")
        for query in query_arr:
            print(query)
            cur.execute(query)
        cur.close()    
        return
        
    def go(self, conn):
        if len(sys.argv[1:]) != 0:
            print("use prec_pool_reindex.py")
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

