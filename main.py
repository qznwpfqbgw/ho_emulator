#%%
from controller import Controller
from log_replayer import Log_Raw_Replayer
from preprocessing import mi_xml_db
from utils.mi2log_to_xml import mi2log_to_xml
#%%
if __name__ == "__main__":
    mi2log_file = 'test/diag_log_sm01_2023-09-21_15-28-46.mi2log'
    db_file = 'test/2023-09-21_UDP_Bandlock_9S_Phone_Brown_sm01_#02.db'
    parameters_file = 'test/br_dl_test_event_params.csv'
    xml_log = 'tmp.xml'
    mi2log_to_xml(mi2log_file, xml_log)
    mi_xml = mi_xml_db(
        xml_log,
        None
    )
    mi_xml.filter = ["LTE_RRC_OTA_Packet", "5G_NR_RRC_OTA_Packet", "LTE_RRC_Serv_Cell_Info"]
    mi_xml.parse_to_db()
    replayer = Log_Raw_Replayer(
        mi2log=mi2log_file,
        real_time=True
    )
    controller = Controller(
        event_params_file=parameters_file,
        db=mi_xml.db
    )
