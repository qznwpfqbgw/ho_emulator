#%%
from log_event_parser import parse_xml_to_db, get_event_through_db
from event import create_event_params
import pandas as pd
import duckdb
import serial
from mobile_insight.monitor.dm_collector import dm_collector_c
import time
class Controller:
    def __init__(self, event_params_file, db, interface):
        self.event_dict = create_event_params(event_params_file)
        event_df = get_event_through_db(db)
        db.close()
        self.config_sched_df = self.calc_event_schedule(event_df)
        self.waiting_time = 0
        self.interface = interface
        
    def set_waiting_time(self, waiting_time):
        self.waiting_time = waiting_time

    def calc_event_schedule(self, event_df):
        df = event_df[1:]
        df['next_start'] = df['start'].shift(-1)
        df['next_type'] = df['type'].shift(-1)
        df['prev_start'] = df['start'].shift(1)
        df['prev_type'] = df['type'].shift(1)
        df = pd.concat(df.apply(self.helper, axis=1).values, ignore_index=True)
        df = df[(pd.isnull(df['overlap_limit_by_next']) | (df['trigger'] < df['overlap_limit_by_next']))]
        df = df[(pd.isnull(df['overlap_limit_by_prev']) | (df['trigger'] > df['overlap_limit_by_prev']))]
        df = df[['bin', 'type', 'trigger']].reset_index()
        df['trigger'] = df['trigger'].apply(lambda x: x.timestamp() - 8 * 60 * 60)
        return df

    def run(self):
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
        '/home/fourcolor/Documents/ho_emulator/test/2023-09-21_UDP_Bandlock_9S_Phone_Brown_sm01_#02.db'
    )
    controller = Controller(
        '/home/fourcolor/Documents/ho_emulator/test/br_dl_test_event_params.csv',
        db,
        'lo'
    )
    #%%
    db.close()
    controller.run()
# %%