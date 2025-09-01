#!/usr/bin/env python3

# Copyright Aug 2025, Philip Wright. All rights reserved.

import sys
import traceback
import sqlite3
import os.path
import time
from datetime import datetime
from pw_utils import mydb_utils
from pw_utils import sat_utils
from pw_utils import score_tables_utils as stu

note = """
for testing use
reads a sat load file(s)
stores in a fake staging table
query the table with sat_query_fake_stage.py
use the fake stage data for matching documentation purposes
"""

class Bork:
    debug = False

    def __init__(self):
        self.country_hash = {}
        # subject scores
        self.s2hash = {}
        pass

    def do_insert(self, conn, tup):
        q = """
        insert into sat_stage_fake(siss_id, test_id, test_component, test_date, data_src, score, date_loaded, last, first, middle, email, address1, city, state, postal, country, birthdate, homephone, status, created, last_update) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        cur = conn.cursor()
        cur.execute(q, tup)
        
    def get_fake_sequence(self, conn):
        q = "insert into sat_stage_fake_seq (test_value) values(?)"
        cur = conn.cursor()
        cur.execute(q, ("test",))
        seq = cur.lastrowid
        return seq
        
    def populate_country_hash(self, conn):
        qhash = "select code2, code3 from sat_country_lu"
        cur = conn.cursor()
        for row in cur.execute(qhash):
            code2, code3 = row
            self.country_hash[code2] = code3
        cur.close()

    def populate_subject_hash(self, conn):
        qhash = "select ncode, acode from sat_lu"
        cur = conn.cursor()
        for row in cur.execute(qhash):
            ncode, acode = row
            self.s2hash[ncode] = acode
        cur.close()

    def satrecord_insert_txt(self, conn, fname, line, line_count, ts):
        length = len(line)
        if length != 2454: # includes newline char at end. don't use strip on .txt flat files
            raise RuntimeError(f"file: {fname} has unexpected line length {length}")

        reportdate = line[2423:2433]
        #icode          = line.mysub(1,6).strip()
        icode          = line[0:6].strip()
        if icode != "5156" and icode != "2397":
            raise RuntimeError(f"rejecting file: {fname} line {line_count} has wrong duke code")

        last           = line[6:41].strip()
        first          = line[41:76].strip()
        mi             = line[76:77].strip()
        sex            = line[77:78].strip()
        fill1          = line[78:79].strip()
        #raceblock      = line[79:90]
        raceblock = "" # don't store it
        fill2          = line[90:95].strip()
        #drace          = line[95:97].strip()
        drace = "" # don't store it
        dob            = line[97:107].strip()
        # ignore it
        #ssn            = line[107:116].strip()
        ssn = ""
        cbsid          = line[116:128].strip()
        sssid          = line[128:158].strip()
        addr1          = line[158:208].strip()
        addr2          = line[208:258].strip()
        city           = line[258:308].strip()
        state          = line[308:310].strip()
        postal         = line[310:327].strip()
        cntyfips       = line[327:332].strip()
        country2       = line[332:334].strip()
        country3 =     self.country_hash.get(country2, "")

        province       = line[334:374].strip()
        phone          = line[374:398].strip()
        email          = line[398:526].strip()
        graddate       = line[526:533].strip()
        geomarket      = line[533:537].strip()
        fai            = line[537:538].strip()
        hsi            = line[538:539].strip()
        fill3          = line[539:559].strip()

        tdate_1        = line[559:569].strip()
        glevel_1       = line[569:571].strip()
        rsi_1          = line[571:572].strip()
        total_1        = line[572:576].strip()
        erws_1         = line[576:579].strip()
        mss_1          = line[579:582].strip()
        rt_1           = line[582:584].strip()
        wlt_1          = line[584:586].strip()
        mt_1           = line[586:590].strip()
        asc_1          = line[590:592].strip()
        ahssc_1        = line[592:594].strip()
        rwc_1          = line[594:596].strip()
        ce_1           = line[596:598].strip()
        ei_1           = line[598:600].strip()
        sec_1          = line[600:602].strip()
        ha_1           = line[602:604].strip()
        pam_1          = line[604:606].strip()
        psda_1         = line[606:608].strip()
        esr_1          = line[608:609].strip()
        esa_1          = line[609:610].strip()
        esw_1          = line[610:611].strip()
        fill4_1        = line[611:613].strip()
        verb_1         = line[613:616].strip()
        math_1         = line[616:619].strip()
        wrsc_1         = line[619:622].strip()
        ess_1          = line[622:624].strip()
        mc_1           = line[624:626].strip()
        essay_1        = line[626:638].strip()
        fill5_1        = line[638:642].strip()


        tdate_2        = line[642:652].strip()
        glevel_2       = line[652:654].strip()
        rsi_2          = line[654:655].strip()
        total_2        = line[655:659].strip()
        erws_2         = line[659:662].strip()
        mss_2          = line[662:665].strip()
        rt_2           = line[664:667].strip()
        wlt_2          = line[667:669].strip()
        mt_2           = line[669:673].strip()
        asc_2          = line[673:675].strip()
        ahssc_2        = line[675:677].strip()
        rwc_2          = line[677:679].strip()
        ce_2           = line[679:681].strip()
        ei_2           = line[681:683].strip()
        sec_2          = line[683:685].strip()
        ha_2           = line[685:687].strip()
        pam_2          = line[687:689].strip()
        psda_2         = line[689:691].strip()
        esr_2          = line[691:692].strip()
        esa_2          = line[692:693].strip()
        esw_2          = line[693:694].strip()
        fill4_2        = line[694:696].strip()
        verb_2         = line[696:699].strip()
        math_2         = line[699:702].strip()
        wrsc_2         = line[702:705].strip()
        ess_2          = line[705:707].strip()
        mc_2           = line[707:709].strip()
        essay_2        = line[709:721].strip()
        fill5_2        = line[721:725].strip()

        tdate_3        = line[725:735].strip()
        glevel_3       = line[735:737].strip()
        rsi_3          = line[737:738].strip()
        total_3        = line[738:742].strip()
        erws_3         = line[742:745].strip()
        mss_3          = line[745:748].strip()
        rt_3           = line[748:750].strip()
        wlt_3          = line[750:752].strip()
        mt_3           = line[752:756].strip()
        asc_3          = line[756:758].strip()
        ahssc_3        = line[758:760].strip()
        rwc_3          = line[760:762].strip()
        ce_3           = line[762:764].strip()
        ei_3           = line[764:766].strip()
        sec_3          = line[766:768].strip()
        ha_3           = line[768:770].strip()
        pam_3          = line[770:772].strip()
        psda_3         = line[772:774].strip()
        esr_3          = line[774:775].strip()
        esa_3          = line[775:776].strip()
        esw_3          = line[776:777].strip()
        fill4_3        = line[777:779].strip()
        verb_3         = line[779:782].strip()
        math_3         = line[782:785].strip()
        wrsc_3         = line[785:788].strip()
        ess_3          = line[788:790].strip()
        mc_3           = line[790:792].strip()
        essay_3        = line[792:804].strip()
        fill5_3        = line[804:808].strip()

        tdate_4        = line[808:818].strip()
        glevel_4       = line[818:820].strip()
        rsi_4          = line[820:821].strip()
        total_4        = line[821:825].strip()
        erws_4         = line[825:828].strip()
        mss_4          = line[828:831].strip()
        rt_4           = line[831:833].strip()
        wlt_4          = line[833:835].strip()
        mt_4           = line[835:839].strip()
        asc_4          = line[839:841].strip()
        ahssc_4        = line[841:843].strip()
        rwc_4          = line[843:845].strip()
        ce_4           = line[845:847].strip()
        ei_4           = line[847:849].strip()
        sec_4          = line[849:851].strip()
        ha_4           = line[851:853].strip()
        pam_4          = line[853:855].strip()
        psda_4         = line[855:857].strip()
        esr_4          = line[857:858].strip()
        esa_4          = line[858:859].strip()
        esw_4          = line[859:860].strip()
        fill4_4        = line[860:862].strip()
        verb_4         = line[862:865].strip()
        math_4         = line[865:868].strip()
        wrsc_4         = line[868:871].strip()
        ess_4          = line[871:873].strip()
        mc_4           = line[873:875].strip()
        essay_4        = line[875:887].strip()
        fill5_4        = line[887:891].strip()

        tdate_5        = line[891:901].strip()
        glevel_5       = line[901:903].strip()
        rsi_5          = line[903:904].strip()
        total_5        = line[904:908].strip()
        erws_5         = line[908:911].strip()
        mss_5          = line[911:914].strip()
        rt_5           = line[914:916].strip()
        wlt_5          = line[916:918].strip()
        mt_5           = line[918:922].strip()
        asc_5          = line[922:924].strip()
        ahssc_5        = line[924:926].strip()
        rwc_5          = line[926:928].strip()
        ce_5           = line[928:930].strip()
        ei_5           = line[930:932].strip()
        sec_5          = line[932:934].strip()
        ha_5           = line[934:936].strip()
        pam_5          = line[936:938].strip()
        psda_5         = line[938:940].strip()
        esr_5          = line[940:941].strip()
        esa_5          = line[941:942].strip()
        esw_5          = line[942:943].strip()
        fill4_5        = line[943:945].strip()
        verb_5         = line[945:948].strip()
        math_5         = line[948:951].strip()
        wrsc_5         = line[951:954].strip()
        ess_5          = line[954:956].strip()
        mc_5           = line[956:958].strip()
        essay_5        = line[958:970].strip()
        fill5_5        = line[970:974].strip()

        tdate_6        = line[974:984].strip()
        glevel_6       = line[984:986].strip()
        rsi_6          = line[986:987].strip()
        total_6        = line[987:991].strip()
        erws_6         = line[991:994].strip()
        mss_6          = line[994:997].strip()
        rt_6           = line[997:999].strip()
        wlt_6          = line[999:1001].strip()
        mt_6           = line[1001:1005].strip()
        asc_6          = line[1005:1007].strip()
        ahssc_6        = line[1007:1009].strip()
        rwc_6          = line[1009:1011].strip()
        ce_6           = line[1011:1013].strip()
        ei_6           = line[1013:1015].strip()
        sec_6          = line[1015:1017].strip()
        ha_6           = line[1017:1019].strip()
        pam_6          = line[1019:1021].strip()
        psda_6         = line[1021:1023].strip()
        esr_6          = line[1023:1024].strip()
        esa_6          = line[1024:1025].strip()
        esw_6          = line[1025:1026].strip()
        fill4_6        = line[1026:1028].strip()
        verb_6         = line[1028:1031].strip()
        math_6         = line[1031:1034].strip()
        wrsc_6         = line[1034:1037].strip()
        ess_6          = line[1037:1039].strip()
        mc_6           = line[1039:1041].strip()
        essay_6        = line[1041:1053].strip()
        fill5_6        = line[1053:1057].strip()

        jtdate_1       = line[1057:1067].strip()
        jglevel_1      = line[1067:1069].strip()
        jrsi_1         = line[1069:1070].strip()
        jt1code_1      = line[1070:1072].strip()
        jt1alph_1 = self.s2hash.get(jt1code_1,"")
        jt1score_1     = line[1072:1075].strip()
        jt1ss1_1       = line[1075:1077].strip()
        jt1ss2_1       = line[1077:1079].strip()
        jt1ss3_1       = line[1079:1081].strip()
        jt2code_1      = line[1081:1083].strip()
        jt2alph_1 = self.s2hash.get(jt2code_1,"")
        jt2score_1     = line[1083:1086].strip()
        jt2ss1_1       = line[1086:1088].strip()
        jt2ss2_1       = line[1088:1090].strip()
        jt2ss3_1       = line[1090:1092].strip()
        jt3code_1      = line[1092:1094].strip()
        jt3alph_1 = self.s2hash.get(jt3code_1,"")
        jt3score_1     = line[1094:1097].strip()
        jt3ss1_1       = line[1097:1099].strip()
        jt3ss2_1       = line[1099:1101].strip()
        jt3ss3_1       = line[1101:1103].strip()

        jtdate_2       = line[1103:1113].strip()
        jglevel_2      = line[1113:1115].strip()
        jrsi_2         = line[1115:1116].strip()
        jt1code_2      = line[1116:1118].strip()
        jt1alph_2 = self.s2hash.get(jt1code_2,"")
        jt1score_2     = line[1118:1121].strip()
        jt1ss1_2       = line[1121:1123].strip()
        jt1ss2_2       = line[1123:1125].strip()
        jt1ss3_2       = line[1125:1127].strip()
        jt2code_2      = line[1127:1129].strip()
        jt2alph_2 = self.s2hash.get(jt2code_2,"")
        jt2score_2     = line[1129:1132].strip()
        jt2ss1_2       = line[1132:1134].strip()
        jt2ss2_2       = line[1134:1136].strip()
        jt2ss3_2       = line[1136:1138].strip()
        jt3code_2      = line[1138:1140].strip()
        jt3alph_2 = self.s2hash.get(jt3code_2,"")
        jt3score_2     = line[1140:1143].strip()
        jt3ss1_2       = line[1143:1145].strip()
        jt3ss2_2       = line[1145:1147].strip()
        jt3ss3_2       = line[1147:1149].strip()

        jtdate_3       = line[1149:1159].strip()
        jglevel_3      = line[1159:1161].strip()
        jrsi_3         = line[1161:1162].strip()
        jt1code_3      = line[1162:1164].strip()
        jt1alph_3 = self.s2hash.get(jt1code_3,"")
        jt1score_3     = line[1164:1167].strip()
        jt1ss1_3       = line[1167:1169].strip()
        jt1ss2_3       = line[1169:1171].strip()
        jt1ss3_3       = line[1171:1173].strip()
        jt2code_3      = line[1173:1175].strip()
        jt2alph_3 = self.s2hash.get(jt2code_3,"")
        jt2score_3     = line[1175:1178].strip()
        jt2ss1_3       = line[1178:1180].strip()
        jt2ss2_3       = line[1180:1182].strip()
        jt2ss3_3       = line[1182:1184].strip()
        jt3code_3      = line[1184:1186].strip()
        jt3alph_3 = self.s2hash.get(jt3code_3,"")
        jt3score_3     = line[1186:1189].strip()
        jt3ss1_3       = line[1189:1191].strip()
        jt3ss2_3       = line[1191:1193].strip()
        jt3ss3_3       = line[1193:1195].strip()

        jtdate_4       = line[1195:1205].strip()
        jglevel_4      = line[1205:1207].strip()
        jrsi_4         = line[1207:1208].strip()
        jt1code_4      = line[1208:1210].strip()
        jt1alph_4 = self.s2hash.get(jt1code_4,"")
        jt1score_4     = line[1210:1213].strip()
        jt1ss1_4       = line[1213:1215].strip()
        jt1ss2_4       = line[1215:1217].strip()
        jt1ss3_4       = line[1217:1219].strip()
        jt2code_4      = line[1219:1221].strip()
        jt2alph_4 = self.s2hash.get(jt2code_4,"")
        jt2score_4     = line[1221:1224].strip()
        jt2ss1_4       = line[1224:1226].strip()
        jt2ss2_4       = line[1226:1228].strip()
        jt2ss3_4       = line[1228:1230].strip()
        jt3code_4      = line[1230:1232].strip()
        jt3alph_4 = self.s2hash.get(jt3code_4,"")
        jt3score_4     = line[1232:1235].strip()
        jt3ss1_4       = line[1235:1237].strip()
        jt3ss2_4       = line[1237:1239].strip()
        jt3ss3_4       = line[1239:1241].strip()

        jtdate_5       = line[1241:1251].strip()
        jglevel_5      = line[1251:1253].strip()
        jrsi_5         = line[1253:1254].strip()
        jt1code_5      = line[1254:1256].strip()
        jt1alph_5 = self.s2hash.get(jt1code_5,"")
        jt1score_5     = line[1256:1259].strip()
        jt1ss1_5       = line[1259:1261].strip()
        jt1ss2_5       = line[1261:1263].strip()
        jt1ss3_5       = line[1263:1265].strip()
        jt2code_5      = line[1265:1267].strip()
        jt2alph_5 = self.s2hash.get(jt2code_5,"")
        jt2score_5     = line[1267:1270].strip()
        jt2ss1_5       = line[1270:1272].strip()
        jt2ss2_5       = line[1272:1274].strip()
        jt2ss3_5       = line[1274:1276].strip()
        jt3code_5      = line[1276:1278].strip()
        jt3alph_5 = self.s2hash.get(jt3code_5,"")
        jt3score_5     = line[1278:1281].strip()
        jt3ss1_5       = line[1281:1283].strip()
        jt3ss2_5       = line[1283:1285].strip()
        jt3ss3_5       = line[1285:1287].strip()

        jtdate_6       = line[1287:1297].strip()
        jglevel_6      = line[1297:1299].strip()
        jrsi_6         = line[1299:1300].strip()
        jt1code_6      = line[1300:1302].strip()
        jt1alph_6 = self.s2hash.get(jt1code_6,"")
        jt1score_6     = line[1302:1305].strip()
        jt1ss1_6       = line[1305:1307].strip()
        jt1ss2_6       = line[1307:1309].strip()
        jt1ss3_6       = line[1309:1311].strip()
        jt2code_6      = line[1311:1313].strip()
        jt2alph_6 = self.s2hash.get(jt2code_6,"")
        jt2score_6     = line[1313:1316].strip()
        jt2ss1_6       = line[1316:1318].strip()
        jt2ss2_6       = line[1318:1320].strip()
        jt2ss3_6       = line[1320:1322].strip()
        jt3code_6      = line[1322:1324].strip()
        jt3alph_6 = self.s2hash.get(jt3code_6,"")
        jt3score_6     = line[1324:1327].strip()
        jt3ss1_6       = line[1327:1329].strip()
        jt3ss2_6       = line[1329:1331].strip()
        jt3ss3_6       = line[1331:1333].strip()

        hscode         = line[1898:1904].strip()
        hsname         = line[1934:1984].strip()
        hsa1           = line[1984:2084].strip()
        hsa2           = line[2084:2164].strip()
        hsa3           = line[2164:2264].strip()
        hscity         = line[2264:2314].strip()
        hsstate        = line[2314:2316].strip()
        hscountry2     = line[2316:2318].strip()
        hsprovince     = line[2318:2358].strip()
        hsphone        = line[2358:2382].strip()
        hstype         = line[2382:2384].strip()

        if addr1 == "":
            mydb_utils.uga_out(sys.stdout, [fname,line_count,last,first,cbsid,hscode,hsname, f"Blank address"])
            
        if email == "":
            mydb_utils.uga_out(sys.stdout, [fname,line_count,last,first,cbsid,hscode,hsname, f"Blank email"])

        vdate_loaded = ts
        tdate_1_ym   = f"{tdate_1[0:4]}{tdate_1[5:7]}"
        

        vstate = ""
        vpostal = ""
        if country2 == "US":
            vstate = state.upper()[0:2]
            postal = postal.upper()[0:5]

        seq = self.get_fake_sequence(conn)
        if tdate_1 != "" and total_1 != "" and erws_1 != "" and mss_1 != "":
            vtdate_1 = mydb_utils.iso_to_date(tdate_1)
            self.do_insert(conn, (seq, "SAT1", "TOTAL",vtdate_1, "ETS", total_1, vdate_loaded, last, first, mi, email, addr1, city, vstate, vpostal, country2, mydb_utils.iso_to_date(dob), phone,"LD", "",""))
            self.do_insert(conn, (seq, "SAT1", "ERWS",vtdate_1, "ETS", erws_1, vdate_loaded, last, first, mi, email, addr1, city, vstate, vpostal, country2, mydb_utils.iso_to_date(dob), phone,"LD", "",""))
            self.do_insert(conn, (seq, "SAT1", "MSS",vtdate_1, "ETS", mss_1, vdate_loaded, last, first, mi, email, addr1, city, vstate, vpostal, country2, mydb_utils.iso_to_date(dob), phone,"LD", "",""))
        
            
    def go(self, conn):
        
        stu.sat_stage_fake_table_drop(conn)
        stu.sat_stage_fake_seq_table_drop(conn)

        stu.sat_stage_fake_seq_table_create(conn)
        stu.sat_stage_fake_table_create(conn)
        
        if len(sys.argv[1:]) == 0:
            print("use _ filename(s).txt")
            sys.exit(-1)

        args = mydb_utils.get_glob_args()
        for fname in args:
            basename = os.path.basename(fname)
            ts = mydb_utils.get_ays_ts()
            if len(fname) < 4:
                print(f"skipping file {fname} with length {len(fname)}")
                continue

            file_suffix = fname[-4:]
            if file_suffix != ".txt":
                print(f"skipping file {fname} with unknown suffix {file_suffix}")
                continue
            print(f"reading file {fname}")
            line_count = 1
            with open(fname, "r", encoding="UTF-8") as file:
                done = False
                while not done:
                    line = file.readline()
                    # eof
                    if len(line) == 0:
                        done = True
                        continue
                    # blank line
                    if len(line) == 1:
                        line_count += 1
                        continue
                    #line = line.strip()  # not use on .txt files...
                    self.satrecord_insert_txt(conn, fname, line, line_count, ts)
                    if (line_count % 100) == 0:
                        conn.commit()
                        print(line_count)
                    line_count += 1
            conn.commit()
        pass # def go


    pass #class

def main():
    conn = None
    try:
        scores_db_name = "scores.db"
        db_dir = mydb_utils.get_sqlite3_db_dir()
        scores_db_file = os.path.join(db_dir, scores_db_name)
        if not os.path.exists(scores_db_file):
            raise RuntimeError(f"scores db file {scores_db_file} not found")
        
        conn = mydb_utils.sqlite3_connect(scores_db_file)
        b = Bork()
        b.populate_country_hash(conn)
        b.populate_subject_hash(conn)
        b.go(conn)
        
    except Exception as err:
        print("Exception ==>{}<== {}".format(err,type(err) ))
        traceback.print_exc()
    finally:
        if not conn is None:
            conn.close()
        pass

main()

