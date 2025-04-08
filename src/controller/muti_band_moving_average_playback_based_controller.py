import numpy as np
from multiprocessing import shared_memory
from utils.band_conversion import *
from controller import Controller
import time
import pandas as pd
import sys
import os
import mmap

sys.path.insert(1, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(1, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '.')))


class Multi_Band_Moving_Average_Playback_Controller(Controller):
    def __init__(self, interface, shm_file, udp_traffic_csv_dict=None, merged_csv_and_band=None, rate_mbit=1000, burst_mbit=100, latency_ms=5, resample_interval=0.3, rolling_wnd_size=1):
        super().__init__(interface, rate_mbit, burst_mbit, latency_ms)

        if udp_traffic_csv_dict is None and merged_csv_and_band is None:
            raise Exception(
                "udp_traffic_csv_dict or merged_csv should be provided")

        self.band = []

        self.result = None
        if merged_csv_and_band:
            self.result = pd.read_csv(merged_csv_and_band['csv'])
            self.result['tx_time_epoch'] = pd.to_datetime(
                self.result['tx_time_epoch']) - pd.Timedelta(hours=8)
            self.result = self.result.set_index('tx_time_epoch').sort_index()
            self.band = merged_csv_and_band['band']
        else:
            for k, udp_traffic_csv in udp_traffic_csv_dict.items():
                self.band.append(k)
                data = pd.read_csv(udp_traffic_csv)[
                    ['tx_time_epoch', 'latency', 'lost']]
                data['tx_time_epoch'] = pd.to_datetime(
                    data['tx_time_epoch'], unit='s') - pd.Timedelta(hours=8)
                latency = data.dropna(subset=['latency']).set_index(
                    'tx_time_epoch')['latency'].sort_index()
                std = latency.rolling(f'{rolling_wnd_size}s').std()
                latency = latency.rolling(f'{rolling_wnd_size}s').mean()
                latency_rs = latency.resample(f'{resample_interval}S')
                std_rs = std.resample(f'{resample_interval}S')
                latency_final = pd.DataFrame({
                    "mean_latency": latency_rs.ffill(),
                    "std_latency": std_rs.ffill()
                })
                lost = data.set_index('tx_time_epoch')['lost'].resample(
                    f'{resample_interval}S').agg(['mean'])
                single_result = pd.concat([latency_final, lost], axis=1)
                single_result.columns = [
                    f'{k}_mean_latency', f'{k}_std_latency', f'{k}_mean_lost']
                
                single_result[f'{k}_mean_latency'] = single_result[f'{k}_mean_latency'] * 1000
                single_result[f'{k}_std_latency'] = single_result[f'{k}_std_latency'] * 1000 / 500
                single_result = single_result.ffill()
                
                if self.result is None:
                    self.result = single_result
                else:
                    self.result = self.result.merge(
                        single_result, on="tx_time_epoch", how="outer")
                    
            self.result = self.result.sort_values(by="tx_time_epoch")
            self.result = self.result.resample(f'{resample_interval}S').apply({
                col: 'ffill' for col in self.result.columns
            })

        self.waiting_time = 0
        self.offset_time = 0
        self.interface = interface
        self.resample_interval = resample_interval
        self.start_log_time = self.result.index[0].timestamp()
        self.start_index = 0  # 新增屬性來儲存起始索引
        self.update_start_index()
        print(shm_file)
        if not os.path.exists(shm_file):
            raise Exception("A: 找不到共享記憶體，等待 predictor 建立...")
        self.shm = mmap.mmap(os.open(shm_file, os.O_RDWR), 9, access=mmap.ACCESS_WRITE)
        print("A: 成功連接到共享記憶體")
        self.band_setting = np.frombuffer(self.shm, dtype=np.uint8, count=1, offset=8)
        self.band_setting.flags.writeable = True
        # self.result.to_csv(f"tmp_{self.interface}.csv")

    def set_waiting_time(self, waiting_time):
        self.waiting_time = waiting_time
        
    def set_offset_time(self, offset_time):
        self.offset_time = offset_time
        self.update_start_index()
        
    def update_start_index(self):
        """根據 offset_time 計算 result 的起始索引"""
        target_time = self.start_log_time + self.offset_time
        target_timestamp = pd.Timestamp(target_time, unit='s')
        nearest_index = self.result.index.get_indexer([target_timestamp], method='nearest')[0]
        self.start_index = nearest_index
        
    def run(self):
        start_time = time.time()
        start_log_time = self.result.index[0].timestamp()
        # self.result.to_csv(f"tmp_{self.interface}.csv")
        for index, row in self.result.iloc[self.start_index:].iterrows():
            current_time = time.time()
            time_diff = (index.timestamp() - start_log_time + self.waiting_time - self.offset_time) - (current_time - start_time)
            # print(f"Time difference: {time_diff:.6f} seconds")
            
            if time_diff < -(self.resample_interval / 2):
                # print("Skipping", time_diff, self.interface)
                time.sleep(0.01)  # 增加一些延遲
                continue
            if time_diff > 0.05:
                # print("Sleeping", time_diff, self.interface)
                time.sleep(time_diff)
            
            
            # print(f"{self.interface} before exec: ", current_time)
            exec_start_time = time.time()
            band_str = band_int_to_str(self.band_setting[0])
            self.run_netem_cmd(row[f'{band_str}_mean_lost']*100, row[f'{band_str}_mean_latency'],
                               row[f'{band_str}_std_latency'], 'normal', self.interface)
            exec_end_time = time.time()
            exec_time = exec_end_time - exec_start_time
            # print(f"{self.interface} after exec: ", time.time())
            print(f"run_netem_cmd execution time: {exec_time:.6f} seconds")
            print(
                f"Index: {index.timestamp()}, Mean Latency: {row[f'{band_str}_mean_latency']}, STD Latency: {row[f'{band_str}_std_latency']}, Lost Ratio: {row[f'{band_str}_mean_lost']}")


if __name__ == '__main__':
    controller = Multi_Band_Moving_Average_Playback_Controller(
        interface='lo',
        # udp_traffic_csv_dict = {
        #     "b3":"/home/fourcolor/Documents/ho_emulator/src/test/multi_band/sm01/udp_dnlk_loss_latency.csv",
        #     "b7":"/home/fourcolor/Documents/ho_emulator/src/test/multi_band/sm02/udp_dnlk_loss_latency.csv",
        #     "b8":"/home/fourcolor/Documents/ho_emulator/src/test/multi_band/sm03/udp_dnlk_loss_latency.csv",
        # },
        merged_csv_and_band={
            'csv': '/home/fourcolor/Documents/ho_emulator/tmp3.csv',
            'band': ['b3', 'b7', 'b8']
        }

    )
    # controller.run()
