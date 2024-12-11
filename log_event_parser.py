from utils import mi_parse_handover
import pandas as pd
import duckdb

def get_event_through_db(db_file):
    db = duckdb.connect(
        db_file
    )
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
    db.close()
    
    return table

if __name__ == "__main__":
    print(get_event_through_db("/home/fourcolor/Documents/ho_emulator/test/2023-09-21_UDP_Bandlock_9S_Phone_Brown_sm01_#02.db"))
# %%
