
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
        self.result['std_latency'] = self.result['std_latency'] * 1000 
        self.result.fillna(method='ffill', inplace=True)
        
        self.waiting_time = 0
        self.interface = interface
        self.resample_interval = resample_interval
        self.start_log_time = self.result.index[0].timestamp()

    def set_waiting_time(self, waiting_time):
        self.waiting_time = waiting_time
        
    def run(self):
        start_time = time.time()
        start_log_time = self.result.index[0].timestamp()
        for row in self.result.itertuples():
            if (row.Index.timestamp() - start_log_time + self.waiting_time) - (time.time() - start_time) < self.resample_interval:
                continue
            if (row.Index.timestamp() - start_log_time + self.waiting_time) - (time.time() - start_time) > 0:
                time.sleep((row.Index.timestamp() - start_log_time + self.waiting_time) - (time.time() - start_time))
            self.run_netem_cmd(row.mean_lost*100, row.mean_latency, row.std_latency, 'normal', self.interface)
            print(f"Index: {row.Index.timestamp()}, Mean Latency: {row.mean_latency}, STD Latency: {row.std_latency}, Lost Ratio: {row.mean_lost}")

if __name__ == '__main__':
    controller = Moving_Average_Playback_Controller(
        '/home/fourcolor/Documents/ho_emulator/src/test/udp_dnlk_loss_latency.csv',
        'lo'
    )
    controller.run()