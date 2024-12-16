from mobile_insight.monitor import OfflineReplayer
from mobile_insight.analyzer import MsgLogger


def mi2log_to_xml(mi2log, out_file):
    replayer = OfflineReplayer()
    replayer.set_input_path(mi2log)
    replayer.enable_log(
        ["LTE_RRC_OTA_Packet", "5G_NR_RRC_OTA_Packet", "LTE_RRC_Serv_Cell_Info"])
    dummper = MsgLogger()
    dummper.set_source(replayer)
    dummper.set_decode_format(MsgLogger.XML)
    dummper.save_decoded_msg_as(out_file)
    dummper.set_dump_type(MsgLogger.FILE_ONLY)
    replayer.run()
