
import sys
import os
sys.path.insert(1,os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(1,os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
print(sys.path)
import pandas as pd
import time
from controller import Controller

class Moving_Average_Playback_Controller(Controller):
    def __init__(self, udp_traffic_csv, interface, rate_mbit=1000, burst_mbit=100, latency_ms=5, resample_interval = 0.3, rolling_wnd_size = 1):
        super().__init__(interface, rate_mbit, burst_mbit, latency_ms)
        data = pd.read_csv(udp_traffic_csv)
        data['tx_time_epoch'] = pd.to_datetime(data['tx_time_epoch'], unit='s') - pd.Timedelta(hours=8)
        latency = data.dropna(subset=['latency']).set_index('tx_time_epoch')['latency'].sort_index()
        std = latency.rolling(f'{rolling_wnd_size}s').std()
        latency = latency.rolling(f'{rolling_wnd_size}s').mean()
        latency_rs = latency.resample(f'{resample_interval}S')
        std_rs = std.resample(f'{resample_interval}S')
        latency_final = pd.DataFrame({
            "mean_latency": latency_rs.ffill(),
            "std_latency": std_rs.ffill()    
        })

        lost = data.set_index('tx_time_epoch')['lost'].resample(f'{resample_interval}S').agg(['mean'])
        self.result = pd.concat([latency_final, lost], axis=1)
        self.result.columns = ['mean_latency', 'std_latency', 'mean_lost']
        self.result['mean_latency'] = self.result['mean_latency'] * 1000 
        self.result['std_latency'] = self.result['std_latency'] * 1000 / 500
        self.result.fillna(method='ffill', inplace=True)
        self.waiting_time = 0
        self.interface = interface
        self.resample_interval = resample_interval
        self.start_log_time = self.result.index[0].timestamp()
        self.start_index = 0  # 新增屬性來儲存起始索引
        self.offset_time = 0
        self.update_start_index()
        # self.result.to_csv(f"tmp_{self.interface}.csv")

    def set_waiting_time(self, waiting_time):
        self.waiting_time = waiting_time
        
    def set_offset_time(self, offset_time):
        self.offset_time = offset_time
        self.update_start_index()
        
    def update_start_index(self):
        target_time = self.start_log_time + self.offset_time
        target_timestamp = pd.Timestamp(target_time, unit='s')
        nearest_index = self.result.index.get_indexer([target_timestamp], method='nearest')[0]
        self.start_index = nearest_index

    def run(self):
        start_time = time.time()
        start_log_time = self.result.index[0].timestamp()
        for index, row in self.result.iloc[self.start_index:].iterrows():
            current_time = time.time()
            time_diff = (index.timestamp() - start_log_time + self.waiting_time - self.offset_time) - (current_time - start_time)
            # print(f"Time difference: {time_diff:.6f} seconds")

            if time_diff < -self.resample_interval:
                # print("Skipping", time_diff, self.interface)
                time.sleep(0.01)  # 增加一些延遲
                continue
            if time_diff > 0.05:
                # print("Sleeping", time_diff, self.interface)
                time.sleep(time_diff)

            # print(f"{self.interface} before exec: ", current_time)
            exec_start_time = time.time()
            self.run_netem_cmd(row['mean_lost'] * 100, row['mean_latency'], row['std_latency'], 'normal', self.interface)
            exec_end_time = time.time()
            exec_time = exec_end_time - exec_start_time
            # print(f"{self.interface} after exec: ", time.time())
            print(f"run_netem_cmd execution time: {exec_time:.6f} seconds")
            print(f"Index: {index.timestamp()}, Mean Latency: {row['mean_latency']}, STD Latency: {row['std_latency']}, Lost Ratio: {row['mean_lost']}")

if __name__ == '__main__':
    controller = Moving_Average_Playback_Controller(
        '/home/fourcolor/Documents/ho_emulator/src/test/udp_dnlk_loss_latency.csv',
        'lo'
    )
    controller.run()