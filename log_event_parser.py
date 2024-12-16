from utils import mi_parse_handover
import pandas as pd
import duckdb
from preprocessing import mi_xml_db

def get_event_through_db(db):
    table, d = mi_parse_handover(
        pd.concat(
            [
                db.sql("select * from RRC_OTA_Packet").df(),
                db.sql("select * from LTE_RRC_Serv_Cell_Info").df(),
            ],
            axis=0,
            ignore_index=True,
        ).sort_values(by="Timestamp", ascending=True)
    )
    return table[['type', 'start']]

def parse_xml_to_db(log_xml):
    mi_xml_db = mi_xml_db(log_xml, None)
    print("Parsing .xml to in-memory database")
    mi_xml_db.filter = ["LTE_RRC_OTA_Packet", "5G_NR_RRC_OTA_Packet", "LTE_RRC_Serv_Cell_Info"]
    mi_xml_db.parse_to_db()
    print("parsing complete")
    return mi_xml_db.db

if __name__ == "__main__":
    db = duckdb.connect("/home/fourcolor/Documents/ho_emulator/test/2023-09-21_UDP_Bandlock_9S_Phone_Brown_sm01_#02.db")
    print(get_event_through_db(db))
# %%
