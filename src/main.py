from controller import *
from log_replayer import *
import argparse
import yaml
import os
import multiprocessing
import signal
from virtual_modem import Virtual_Multi_Band_Modem, Virtual_Modem

processes = []

def signal_handler(signum, frame):
    global processes
    time.sleep(1)
    for p in processes:
        if p._popen:
            p.terminate()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGTSTP, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config_file", default="config.yml", help="Config file (yaml)"
    )
    args = parser.parse_args()

    with open(args.config_file, "r") as f:
        config = yaml.safe_load(f)

    replayers, controllers = [], []
    controller_cls = {
        "Playback_Controller": Playback_Controller,
        "Moving_Average_Playback_Controller": Moving_Average_Playback_Controller,
        "Multi_Band_Moving_Average_Playback_Controller": Multi_Band_Moving_Average_Playback_Controller,
    }
    replayer_cls = {
        "Log_Raw_Replayer": Log_Raw_Replayer,
        "Multi_Band_Log_Raw_Replayer": Multi_Band_Log_Raw_Replayer,
    }
    callback_cls = {
        "Virtual_Modem": Virtual_Modem,
        "Virtual_Multi_Band_Modem": Virtual_Multi_Band_Modem,
    }
    for controller_info in config["Controllers"]:
        k = controller_info["type"]
        if k == "Multi_Band_Moving_Average_Playback_Controller":
            udp_traffic_csv_dict = {}
            for band_info in controller_info["band_candidates"]:
                if not os.path.isfile(band_info["traffic"]):
                    raise Exception(
                        f"Please make sure {band_info['traffic']} is existed !!!"
                    )
                udp_traffic_csv_dict[band_info["band"]] = band_info["traffic"]
            controllers.append(
                Multi_Band_Moving_Average_Playback_Controller(
                    interface=controller_info["interface"],
                    shm_file=controller_info["shm_file"],
                    udp_traffic_csv_dict=udp_traffic_csv_dict,
                    merged_csv_and_band=None,
                    rate_mbit=controller_info["rate_mbit"],
                    burst_mbit=controller_info["burst_mbit"],
                    latency_ms=controller_info["latency_ms"],
                    resample_interval=controller_info["resample_s"],
                    rolling_wnd_size=controller_info["rolling_wnd_size_s"],
                )
            )
        elif k == "Moving_Average_Playback_Controller":
            controllers.append(
                Moving_Average_Playback_Controller(
                    udp_traffic_csv=controller_info["traffic"],
                    interface=controller_info["interface"],
                    rate_mbit=controller_info["rate_mbit"],
                    burst_mbit=controller_info["burst_mbit"],
                    latency_ms=controller_info["latency_ms"],
                    resample_interval=controller_info["resample_s"],
                    rolling_wnd_size=controller_info["rolling_wnd_size_s"],
                )
            )
        else:
            controllers.append(
                Playback_Controller(
                    udp_traffic_csv=controller_info["traffic"],
                    interface=controller_info["interface"],
                    rate_mbit=controller_info["rate_mbit"],
                    burst_mbit=controller_info["burst_mbit"],
                    latency_ms=controller_info["latency_ms"],
                    resample_interval=controller_info["resample_s"],
                )
            )

    for replayer_info in config["Replayers"]:
        
        modem = replayer_info["modem_type"]
        if modem == "Virtual_Modem":
            modem = Virtual_Modem(replayer_info["virt_serial_ports"])
        elif modem == "Virtual_Multi_Band_Modem":
            modem = Virtual_Multi_Band_Modem(
                ser=replayer_info["virt_serial_ports"],
                shm_file=replayer_info["shm_file"],
            )
            
        k = replayer_info["type"]
        if k == "Log_Raw_Replayer":
            replayer = Log_Raw_Replayer(mi2log=replayer_info["mi2log"], real_time=True)
            replayer.add_subscriber_callback(modem.replayer_callback)
            replayers.append(replayer)
            
        elif k == "Multi_Band_Log_Raw_Replayer":
            if os.path.isfile(replayer_info["merged_mi2log"]):
                replayer = Multi_Band_Log_Raw_Replayer(
                    mi2log_merged_db=replayer_info["merged_mi2log"], real_time=True
                )
                replayer.add_subscriber_callback(modem.replayer_callback)
                replayers.append(replayer)
            else:
                for band_info in replayer_info["band_candidates"]:
                    replayer = Log_Raw_Replayer(mi2log=band_info["mi2log"], real_time=True, band=band_info["band"])
                    replayer.add_subscriber_callback(modem.replayer_callback)
                    replayers.append(replayer)

    max_start_time = max(
        filter(
            None,
            [
                replayer.get_start_time() for replayer in replayers if replayer
            ]
            + [
                controller.get_start_time() for controller in controllers if controller
            ],
        )
    )

    # 顯示每個 start_time
    for replayer in replayers:
        if replayer:
            print(f"Replayer start time: {replayer.get_start_time()}")

    for controller in controllers:
        if controller:
            print(f"Controller start time: {controller.get_start_time()}")     
    
    for replayer in replayers:
        if replayer:
            offset_time = max(max_start_time - replayer.get_start_time(), config['Global']["offset_time"] - replayer.get_start_time())
            print(f"Replayer offset time: {offset_time}")
            # if abs(offset_time) > 60 * 5:
            #     raise ValueError("Ensure traffic and mi2log are from the same source")
            replayer.set_offset_time(offset_time)

    for controller in controllers:
        if controller:
            offset_time = max(max_start_time - controller.get_start_time(), config['Global']["offset_time"] - controller.get_start_time())
            print(f"Controller offset time: {offset_time}")
            # if abs(offset_time) > 60 * 5:
            #     raise ValueError("Ensure traffic and mi2log are from the same source")
            controller.set_offset_time(offset_time)
            
    # 啟動進程
    for component in replayers + controllers:
        if component:
            p = multiprocessing.Process(target=component.run)
            processes.append(p)
            p.start()
    
    print("duration",config['Global']["duration"])
    time.sleep(config['Global']["duration"])

    for p in processes:
        if config['Global']["duration"] != 0:
            p.terminate()
        else:
            p.join()
    
    signal_handler(0,0)
