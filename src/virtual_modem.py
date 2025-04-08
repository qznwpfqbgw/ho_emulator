import serial
from multiprocessing import shared_memory
import time
import numpy as np
import os
import mmap
from utils.band_conversion import *
class Virtual_Modem():
    def __init__(self, ser) -> None:
        self.ser = serial.Serial(ser)
        
    def replayer_callback(self, msg):
        self.ser.write(msg[0])
        
class Virtual_Multi_Band_Modem():
    def __init__(self, ser, shm_file) -> None:
        self.ser = serial.Serial(ser)
        print(ser)
        
        if not os.path.exists(shm_file):
            raise Exception("A: 找不到共享記憶體，等待 predictor 建立...")
        self.shm = mmap.mmap(os.open(shm_file, os.O_RDWR), 9, access=mmap.ACCESS_WRITE)
        print("A: 成功連接到共享記憶體")
        self.band_setting = np.frombuffer(self.shm, dtype=np.uint8, count=1, offset=8)
        self.band_setting.flags.writeable = True
        
    def replayer_callback(self, msg):
        if band_str_to_int(msg[2]) == (self.band_setting[0] & 31):
            # print("Matched")
            self.ser.write(msg[0])