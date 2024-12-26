#%%
from log_event_parser import parse_xml_to_db, get_event_through_db
from event import create_event_params, Event
import pandas as pd
import duckdb
import serial
from mobile_insight.monitor.dm_collector import dm_collector_c
import time
import subprocess
class Controller:
    def __init__(self, event_params_file, db, interface):
        self.event_dict: dict[str: Event] = create_event_params(event_params_file)
        event_df = get_event_through_db(db)
        self.config_sched_df = self.calc_event_schedule(event_df)
        self.waiting_time = 0
        self.interface = interface
        proc = subprocess.Popen(
            [
                "tc",
                "qdisc",
                "add",
                "dev",
                self.interface,
                "root",
                "netem",
                "delay",
                "0ms",
                "loss",
                "0%",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = proc.communicate()
        if stdout:
            print(stdout)
        if stderr:
            print(stderr)

    def set_waiting_time(self, waiting_time):
        self.waiting_time = waiting_time

    def calc_event_schedule(self, event_df):
        # df = event_df[1:]
        print(event_df)
        rows = []
        for i in range(len(event_df)):
            rows.append(event_df.iloc[i])
            
            # 當前事件結束時間
            cur_event_impact_end = event_df.iloc[i]['start'] + pd.to_timedelta(
                max(self.event_dict[event_df.iloc[i]['type']].impact_params.keys()), unit='s')
            
            # 穩定事件開始時間
            stable_event_impact_start = cur_event_impact_end + pd.to_timedelta(0.1, unit='s')
            # print(event_df.iloc[i]['start'], max(self.event_dict[event_df.iloc[i]['type']].impact_params.keys()), cur_event_impact_end, stable_event_impact_start)
            # 下一個事件影響開始時間
            next_event_impact_start = cur_event_impact_end + pd.to_timedelta(0.2, unit='s')
            
            if i + 1 < len(event_df):
                next_event_impact_start = event_df.iloc[i + 1]['start'] + pd.to_timedelta(
                    min(self.event_dict[event_df.iloc[i + 1]['type']].impact_params.keys()), unit='s')
            
            # 檢查是否符合插入條件
            if stable_event_impact_start < next_event_impact_start and stable_event_impact_start > cur_event_impact_end:
                rows.append(pd.Series({'type': "Stable", "start": stable_event_impact_start}, index=['type', 'start']))

        # 生成新的 DataFrame
        df = pd.DataFrame(rows)
        df['next_start'] = df['start'].shift(-1)
        df['next_type'] = df['type'].shift(-1)
        df['prev_start'] = df['start'].shift(1)
        df['prev_type'] = df['type'].shift(1)
        df = pd.concat(df.apply(self.helper, axis=1).values, ignore_index=True)
        df = df[(pd.isnull(df['overlap_limit_by_next']) | (df['trigger'] < df['overlap_limit_by_next']))]
        df = df[(pd.isnull(df['overlap_limit_by_prev']) | (df['trigger'] > df['overlap_limit_by_prev']))]
        df = df[['bin', 'type', 'trigger']].reset_index()
        # df['trigger'] = df['trigger'].apply(lambda x: x.timestamp() - 8 * 60 * 60)
        df['trigger'] = df['trigger'].apply(lambda x: x.timestamp())
        return df

    def run(self):
        self.event_dict['Stable'].set_effect_params(0.0, self.interface)
        start_time = time.time()
        start_log_time = self.config_sched_df['trigger'][0]
        for r in self.config_sched_df.itertuples():
            if (r.trigger - start_log_time + self.waiting_time) - (time.time() - start_time) > 0.05:
                # print("controller",(r.trigger - start_log_time + self.waiting_time) - (time.time() - start_time))
                time.sleep((r.trigger - start_log_time + self.waiting_time) - (time.time() - start_time))
            self.event_dict[r.type].set_effect_params(r.bin, self.interface)

    def helper(self, r):
        df = pd.DataFrame.from_dict(self.event_dict[r['type']].impact_params, orient='index')
        df.index.names = ['bin']
        df.reset_index(inplace=True) 
        df['type'] = r['type']
        df['start'] = r['start']
        df['trigger'] = df['start'] + pd.to_timedelta(df['bin'].astype(float), unit='s')
        if pd.notna(r['next_type']):
            min_bin = min(self.event_dict[r['next_type']].impact_params.keys())
            df['overlap_limit_by_next'] = min(
                r['next_start'] + pd.to_timedelta(min_bin, unit='s'), 
                r['start'] + (r['next_start'] - r['start'])/2
            )
        if pd.notna(r['prev_type']):
            max_bin = max(self.event_dict[r['prev_type']].impact_params.keys())
            df['overlap_limit_by_prev'] = max(
                r['prev_start'] + pd.to_timedelta(max_bin, unit='s'),
                r['prev_start'] + (r['start'] - r['prev_start'])/2
            )
        return df
#%%
if __name__ == "__main__":
    db = duckdb.connect(
        'test/2023-09-21_UDP_Bandlock_9S_Phone_Brown_sm01_#02.db'
    )
    controller = Controller(
        'test/br_dl_test_event_params.csv',
        db,
        'eth0'
    )
    #%%
    db.close()
    #%%
    controller.run()
# %%