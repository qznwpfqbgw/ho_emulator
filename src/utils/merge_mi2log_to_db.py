import duckdb
from mobile_insight.monitor.dm_collector import dm_collector_c, DMLogPacket, FormatError
import traceback
import sys

def merge_mi2log_to_db(mi2log_dict, db):
    DMLogPacket.init({})
    con = duckdb.connect(db)
          
    con.execute(f'''
    CREATE TABLE log_data (
        timestamp DOUBLE,
        raw_data BLOB,
        band VARCHAR
    )
    ''')
    
    for band, log in mi2log_dict.items():
        input_file = open(log, "rb")
        dm_collector_c.reset()
        dm_collector_c.set_filtered(list(set(dm_collector_c.log_packet_types)))
        raw_data_to_send = b''
        while True:
            s = input_file.read(1)
            raw_data_to_send += s
            if s:
                dm_collector_c.feed_binary(s)
            decoded = dm_collector_c.receive_log_packet(
                True,  # skip decode
                False, # timestamp
            )
            if not s and not decoded:
                # EOF encountered and no message can be received any more
                break
            if decoded:
                try:
                    if not decoded[0]:
                        continue
                    result = next((t for t in decoded if t[0] == "timestamp"), None)
                    if result is not None:
                        con.execute("INSERT INTO log_data VALUES (?, ?, ?)", (result[1].timestamp(), raw_data_to_send, band))
                        raw_data_to_send = b''
                except Exception as e:
                    traceback.print_exc()
                    sys.exit(-1)
        con.sql(f"""
            CREATE TABLE temp_table AS 
            SELECT * FROM log_data ORDER BY timestamp;
            DROP TABLE log_data;
            ALTER TABLE temp_table RENAME TO log_data;
        """)
if __name__ == "__main__":
    mi2log_dict = {
        # "b1_b3": "/home/fourcolor/Documents/ho_emulator/src/test/multi_band/sm02/diag_log_sm02_2024-06-18_15-53-03.mi2log",
        # "b1_b7": "/home/fourcolor/Documents/ho_emulator/src/test/multi_band/sm03/diag_log_sm03_2024-06-18_15-53-03.mi2log",
        # "b1_b8": "/home/fourcolor/Documents/ho_emulator/src/test/multi_band/sm04/diag_log_sm04_2024-06-18_15-53-03.mi2log",
        # "b3_b7": "/home/fourcolor/Documents/ho_emulator/src/test/multi_band/sm05/diag_log_sm05_2024-06-18_15-53-03.mi2log",
        # "b3_b8": "/home/fourcolor/Documents/ho_emulator/src/test/multi_band/sm06/diag_log_sm06_2024-06-18_15-53-03.mi2log",
        # "b7_b8": "/home/fourcolor/Documents/ho_emulator/src/test/multi_band/sm07/diag_log_sm07_2024-06-18_15-53-03.mi2log"
        "b1_b3_b7_b8": "/home/fourcolor/Documents/ho_emulator/src/test/multi_band/sm01/diag_log_sm01_2024-06-18_15-53-03.mi2log"
    }
    merge_mi2log_to_db(mi2log_dict, "sm01.db")
    # con = duckdb.connect("mi2log.db")
    # con.execute("SELECT * FROM log_data")
    # print(con.fetch_df())