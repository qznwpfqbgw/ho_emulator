import serial
class Virtual_Modem():
    def __init__(self, ser) -> None:
        self.ser = serial.Serial(ser)
        
    def replayer_callback(self, msg):
        self.ser.write(msg[0])