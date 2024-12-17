#%%
from controller import Controller
from log_replayer import Log_Raw_Replayer
from preprocessing import mi_xml_db
from utils.mi2log_to_xml import mi2log_to_xml
import argparse
import yaml
import os
import duckdb
import multiprocessing
import signal
#%%
processes: list[multiprocessing.Process] = []

def signal_handler(signum, frame):
    for p in processes:
        if p.is_alive():
            p.terminate() 

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGTSTP, signal_handler)
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file', default='config.yml', help="Config file (yaml)")
    args = parser.parse_args()
    
    with open(args.config_file,'r') as f:
        config = yaml.safe_load(f)
        
    mi2log_file = None
    db_file = None
    parameters_file = None
    xml_log = None
    replayer = None
    controller = None
    if config['Global']['mi2log'] is not None and os.path.isfile(config['Global']['mi2log']):
        mi2log_file = config['Global']['mi2log']
    if config['Global']['xml_log'] is not None and os.path.isfile(config['Global']['xml_log']):
        xml_log = config['Global']['xml_log']
    if config['Global']['db_log'] is not None and os.path.isfile(config['Global']['db_log']):
        db_file = config['Global']['db_log']    

    if xml_log is None and mi2log_file is None:
        raise Exception("One of mi2log and xml_log should be provided")
    if xml_log is None:
        mi2log_to_xml(mi2log_file, xml_log)
        
    if config['Replayer']['enable']:
        replayer = Log_Raw_Replayer(
            mi2log=mi2log_file,
            real_time=True
        )
    if config['Controller']['enable']:
        if db_file is None:
            mi_xml = mi_xml_db(
                xml_log,
                None
            )
            mi_xml.filter = ["LTE_RRC_OTA_Packet", "5G_NR_RRC_OTA_Packet", "LTE_RRC_Serv_Cell_Info"]
            mi_xml.parse_to_db()
            db = mi_xml.db
        else:
            db = duckdb.connect(db_file)
        
        parameters_file = config['Controller']['parameters_file']
            
        controller = Controller(
            event_params_file=parameters_file,
            db=db,
            interface=config['Controller']['inrerface']
        )
        
        db.close()
        
    if config['Replayer']['enable'] and config['Controller']['enable']:
        controller_waiting_time = controller.config_sched_df['trigger'][0] - replayer.get_start_time()
        if controller_waiting_time < 0:
            raise Exception("please make sure the db log and mi2log is the same source")
        controller.set_waiting_time(controller_waiting_time)
    
    if replayer:
        processes.append(multiprocessing.Process(target=replayer.run))
    if controller:
        processes.append(multiprocessing.Process(target=controller.run))
        
    for p in processes:
        p.start()
    
    
    # 等待所有進程完成
    for p in processes:
        p.join()

# %%
