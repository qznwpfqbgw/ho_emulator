import time
import sys
import traceback
from utils.band_conversion import *
import duckdb
class Multi_Band_Log_Raw_Replayer:
    def __init__(self, mi2log_merged_db, real_time, offset_time = 0) -> None:
        self.con = duckdb.connect(mi2log_merged_db)
        self.log_data = self.con.execute("SELECT * FROM log_data ORDER BY timestamp").fetchdf()
        self.real_time = real_time
        self.subscriber_callbacks = []
        self.waiting_time = 0
        self.offset_time = offset_time
        self.start_index = 0
        self.update_start_index()

    def add_subscriber_callback(self, callback):
        self.subscriber_callbacks.append(callback)
        
    def set_waiting_time(self, waiting_time):
        self.waiting_time = waiting_time
        
    def set_offset_time(self, offset_time):
        self.offset_time = offset_time
        self.update_start_index()
        
    def get_start_time(self):
        return self.log_data['timestamp'].iloc[0]

    def update_start_index(self):
        """根據 offset_time 計算 log_data 的起始索引"""
        start_time = self.log_data['timestamp'].iloc[0]
        target_time = start_time + self.offset_time
        self.start_index = self.log_data[self.log_data['timestamp'] >= target_time].index[0]

    def run(self):
        with open("tmp_time_sync.out", "a") as f:
            f.write(f"start: {self.log_data['timestamp'].iloc[0]} offset: {self.offset_time} current: {time.time()}\n")
        try:
            start_log_time = self.log_data['timestamp'].iloc[0]
            start_send_real_time = time.time()
            
            for index, row in self.log_data.iloc[self.start_index:].iterrows():
                cur_log_time = row['timestamp']
                
                if self.real_time:
                    if (cur_log_time - start_log_time < self.offset_time):
                        continue
                    if (cur_log_time - start_log_time + self.waiting_time - self.offset_time) - (time.time() - start_send_real_time) > 0.15:
                        # print("Sleeping for", (cur_log_time - start_log_time + self.waiting_time - self.offset_time) - (time.time() - start_send_real_time))
                        time.sleep((cur_log_time - start_log_time + self.waiting_time - self.offset_time) - (time.time() - start_send_real_time) - 0.05)
                        
                for callback in self.subscriber_callbacks:
                    callback((row['raw_data'], None, row['band']))
                    
        except Exception as e:
            traceback.print_exc()
            sys.exit(-1)


if __name__ == "__main__":
    replayer = Multi_Band_Log_Raw_Replayer(
        '/home/fourcolor/Documents/ho_emulator/mi2log.db',
        True,
        0
    )
    import serial
    ser = serial.Serial("/dev/ttyV3")
    def callback(msg):
        ser.write(msg[0])
        # print(msg[1].decode_xml())
    replayer.add_subscriber_callback(callback)
    replayer.run()