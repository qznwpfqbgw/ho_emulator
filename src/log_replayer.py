from mobile_insight.monitor.dm_collector import dm_collector_c, DMLogPacket, FormatError
import time
import sys
import traceback
import argparse
from multiprocessing import Process

class Log_Raw_Replayer:
    def __init__(self, mi2log, real_time) -> None:
        self.log_file_path = mi2log
        self.real_time = real_time
        DMLogPacket.init({})
        self.subscriber_callbacks = []
        self.waiting_time = 0
        
    def add_subscriber_callback(self, callback):
        self.subscriber_callbacks.append(callback)
        
    def set_waiting_time(self, waiting_time):
        self.waiting_time = waiting_time
        
    def get_start_time(self):
        self._input_file = open(self.log_file_path, "rb")
        dm_collector_c.reset()
        dm_collector_c.set_filtered(list(set(dm_collector_c.log_packet_types)))
        while True:
            s = self._input_file.read(64)
            if s:
                dm_collector_c.feed_binary(s)
            decoded = dm_collector_c.receive_log_packet(
                True,  # skip decode
                False, # timestamp
            )
            if not s and not decoded:
                # EOF encountered and no message can be received any more
                break
            if decoded:
                try:
                    if not decoded[0]:
                        continue
                    result = next((t for t in decoded if t[0] == "timestamp"), None)
                    if result is not None:
                        self._input_file.close()
                        return result[1].timestamp()
                except Exception as e:
                    traceback.print_exc()
                    sys.exit(-1)
                        
    def run(self):
        try:
            self._input_file = open(self.log_file_path, "rb")
            dm_collector_c.reset()
            dm_collector_c.set_filtered(list(set(dm_collector_c.log_packet_types)))
            raw_data_to_send = b''
            start_log_time = None
            cur_log_time = None
            start_send_real_time = None
            while True:
                s = self._input_file.read(64)
                raw_data_to_send += s
                if s:
                    dm_collector_c.feed_binary(s)

                decoded = dm_collector_c.receive_log_packet(
                    True,  # skip decode
                    False, # timestamp
                )

                if not s and not decoded:
                    # EOF encountered and no message can be received any more
                    break

                if decoded:
                    try:
                        if not decoded[0]:
                            continue

                        result = next((t for t in decoded if t[0] == "timestamp"), None)
                        if result is not None:
                            cur_log_time = result[1].timestamp()
                            if start_log_time is None:
                                start_send_real_time = time.time()
                                start_log_time = result[1].timestamp()
                        else:
                            raise Exception("No tuple found with timestamp")
                        
                        if self.real_time:
                            if (cur_log_time - start_log_time + self.waiting_time) - (time.time() - start_send_real_time) > 0.05:
                                # print("sleep:", (cur_log_time - start_log_time) - (time.time() - start_send_real_time), flush=True)
                                time.sleep((cur_log_time - start_log_time) - (time.time() - start_send_real_time))
                        # print(time.time() -  start_send_real_time, cur_log_time - start_log_time)
                        for callback in self.subscriber_callbacks:
                            packet = DMLogPacket(decoded)
                            callback((raw_data_to_send, packet))
                        # print(next((t for t in decoded if t[0] == "type_id"), None)[1],len(raw_data_to_send))
                        raw_data_to_send = b''
                    except FormatError as e:
                        print(("FormatError: ", e))
            self._input_file.close()
        except Exception as e:
                traceback.print_exc()
                sys.exit(-1)

if __name__ == "__main__":
    replayer1 = Log_Raw_Replayer(
        'test/diag_log_sm01_2023-09-21_15-28-46.mi2log',
        True
    )

    replayer2 = Log_Raw_Replayer(
        'test/diag_log_sm00_2023-09-21_14-29-12.mi2log',
        True
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=str, nargs='+', help="device: e.g. ttyV1 ttyV3")
    args = parser.parse_args()

    port1, port2 = args.port[0], args.port[1]

    import serial
    ser1 = serial.Serial("/tmp/" + port1)
    ser2 = serial.Serial("/tmp/" + port2)

    def callback1(msg):
        ser1.write(msg[0])
        
    def callback2(msg):
        ser2.write(msg[0])

    replayer1.add_subscriber_callback(callback1)
    replayer2.add_subscriber_callback(callback2)

    # using multi-processing to run prediction model inference on both dual radios
    p1 = Process(target=replayer1.run())     
    p2 = Process(target=replayer2.run())
    
    p1.start()
    p2.start()

    try:
        while True:
            a = 1
    except KeyboardInterrupt:
        # Stop Record
        print('Main process received KeyboardInterrupt')
        p1.join()
        p2.join()
        time.sleep(1)
        print("Process killed, closed.")
        sys.exit()