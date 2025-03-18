from controller import *
from log_replayer import Log_Raw_Replayer
from preprocessing import mi_xml_db
from utils.mi2log_to_xml import mi2log_to_xml
import argparse
import yaml
import os
import duckdb
import multiprocessing
import signal
from virtual_modem import Virtual_Modem

def signal_handler(signum, frame):
    for p in processes:
        if p.is_alive():
            p.terminate()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGTSTP, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config_file", default="config.yml", help="Config file (yaml)")
    args = parser.parse_args()

    with open(args.config_file, "r") as f:
        config = yaml.safe_load(f)

    # 初始化變數
    mi2log_file = config["Global"].get("mi2log")
    xml_log = config["Global"].get("xml_log")
    db_file = config["Global"].get("db_log")

    if not (mi2log_file or xml_log):
        raise ValueError("One of mi2log or xml_log must be provided")

    processes = []
    replayer, ul_controller, dl_controller = None, None, None

    # 設置 Replayer
    if config["Replayer"]["enable"]:
        replayer = Log_Raw_Replayer(mi2log=mi2log_file, real_time=True)
        virt_modem = Virtual_Modem(config["Replayer"]["virt_serial_port"])
        replayer.add_subscriber_callback(virt_modem.replayer_callback)
    
    # 處理 Profile-Based 和 Playback 互斥問題
    if config["DL_Profile_Based_Controller"]["enable"] and config["DL_Playback_Controller"]["enable"]:
        raise ValueError("Cannot enable both DL_Profile_Based_Controller and DL_Playback_Controller")
    
    # 設置 DL 控制器
    dl_controllers = {
        "DL_Profile_Based_Controller": Profile_Based_Controller,
        "DL_Playback_Controller": Playback_Controller,
        "DL_Moving_Average_Playback_Controller": Moving_Average_Playback_Controller,
    }
    
    for key, controller_class in dl_controllers.items():
        if config[key]["enable"]:
            if key == "DL_Profile_Based_Controller":
                if not (db_file and xml_log):
                    raise ValueError("Please provide db_log and xml_log name")
                if not os.path.isfile(xml_log):
                    mi2log_to_xml(mi2log_file, xml_log)
                if not os.path.isfile(db_file):
                    mi_xml = mi_xml_db(xml_log, db_file)
                    mi_xml.filter = ["LTE_RRC_OTA_Packet", "5G_NR_RRC_OTA_Packet", "LTE_RRC_Serv_Cell_Info"]
                    mi_xml.parse_to_db()
                    mi_xml.run_extension()
                    db = mi_xml.db
                else:
                    db = duckdb.connect(db_file)
                
                dl_controller = controller_class(
                    event_params_file=config[key]["parameters_file"],
                    db=db,
                    interface=config[key]["interface"],
                    perfect_stable=config[key]["perfect_stable"],
                    rate_mbit=config[key]["rate_mbit"],
                    burst_mbit=config[key]["burst_mbit"],
                    latency_ms=config[key]["latency_ms"]
                )
                db.close()
            else:
                if not os.path.isfile(config[key]["udp_traffic_csv"]):
                    raise ValueError("Please provide udp_traffic_csv")
                dl_controller = controller_class(
                    udp_traffic_csv=config[key]["udp_traffic_csv"],
                    rate_mbit=config[key]["rate_mbit"],
                    burst_mbit=config[key]["burst_mbit"],
                    latency_ms=config[key]["latency_ms"],
                    interface=config[key]["interface"],
                    resample_interval=config[key].get("resample_s"),
                    rolling_wnd_size=config[key].get("rolling_wnd_size_s")
                )
            break
    
    # 設置 UL 控制器
    if config["UL_Moving_Average_Playback_Controller"]["enable"]:
        ul_config = config["UL_Moving_Average_Playback_Controller"]
        if not os.path.isfile(ul_config["udp_traffic_csv"]):
            raise ValueError("Please provide udp_traffic_csv")
        ul_controller = Moving_Average_Playback_Controller(
            udp_traffic_csv=ul_config["udp_traffic_csv"],
            rate_mbit=ul_config["rate_mbit"],
            burst_mbit=ul_config["burst_mbit"],
            latency_ms=ul_config["latency_ms"],
            interface=ul_config["interface"],
            resample_interval=ul_config["resample_s"],
            rolling_wnd_size=ul_config["rolling_wnd_size_s"]
        )

    # 設置等待時間 (統一處理)
    
    min_start_time = min(filter(None, [
        replayer.get_start_time() if replayer else None,
        dl_controller.get_start_time() if dl_controller else None,
        ul_controller.get_start_time() if ul_controller else None
    ]))
    for k, component in {"Replayer": replayer, "DL": dl_controller, "UL": ul_controller}.items():
        if component:
            waiting_time = component.get_start_time() - min_start_time
            print(f"{k} waiting time: {waiting_time}")
            if waiting_time < 0:
                raise ValueError("Ensure db log and mi2log are from the same source")
            component.set_waiting_time(waiting_time)

    # 啟動進程
    for component in [replayer, dl_controller, ul_controller]:
        if component:
            p = multiprocessing.Process(target=component.run)
            processes.append(p)
            p.start()
    
    for p in processes:
        p.join()
