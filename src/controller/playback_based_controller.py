
import sys
import os
sys.path.insert(1,os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(1,os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
print(sys.path)
import pandas as pd
import time
from controller import Controller

class Playback_Controller(Controller):
    def __init__(self, udp_traffic_csv, interface, rate_mbit=1000, burst_mbit=100, latency_ms=5, resample_interval = 0.3):
        super().__init__(interface)
        data = pd.read_csv(udp_traffic_csv)
        data['tx_time_epoch'] = pd.to_datetime(data['tx_time_epoch'], unit='s')
        latency = data[data['lost'] != True].set_index('tx_time_epoch')['latency'].resample(f'{resample_interval}S').agg(['mean', 'std'])
        lost = data.set_index('tx_time_epoch')['lost'].resample(f'{resample_interval}S').agg(['mean'])
        self.result = pd.concat([latency, lost], axis=1)
        self.result.columns = ['mean_latency', 'std_latency', 'mean_lost']
        self.result['mean_latency'] = self.result['mean_latency'] * 1000
        self.result['std_latency'] = self.result['std_latency'] * 1000 / 10
        self.result.fillna(method='ffill', inplace=True)
        
        self.waiting_time = 0
        self.interface = interface
        
    def set_waiting_time(self, waiting_time):
        self.waiting_time = waiting_time
        
    def run(self):
        start_time = time.time()
        start_log_time = self.result.index[0].timestamp()
        for row in self.result.itertuples():
            if (row.Index.timestamp() - start_log_time + self.waiting_time) - (time.time() - start_time) > 0.05:
                time.sleep((row.Index.timestamp() - start_log_time + self.waiting_time) - (time.time() - start_time))
            self.run_netem_cmd(row.mean_lost, row.mean_latency, row.std_latency, 'normal', self.interface)
            print(f"Index: {row.Index.timestamp()}, Mean Latency: {row.mean_latency}, STD Latency: {row.std_latency}, Lost Ratio: {row.mean_lost}")

if __name__ == '__main__':
    controller = Playback_Controller(
        '/home/fourcolor/Documents/ho_emulator/src/test/udp_dnlk_loss_latency.csv',
        'lo'
    )
    controller.run()