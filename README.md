# HO Emulator
## Usage
1. Execute `manage_virtual_serial.sh start` to create virtual serial port
```
$ ./src/manage_virtual_serial.sh start
```
2. Check the actual pty port (just choose one to check, the other one should be used by your application)
```
$ ls -la /tmp/ttyV0
/tmp/ttyV0 -> /dev/pts/2
```
3. Modify the config.yml
```
...
Replayer:
  enable: True
  virt_serial_port: /dev/pts/2
```
4. Run Docker container 
```
$ docker-compose up -d # or docker compose up -d
```
5. Run the replayer and controller
```
$ docker-compose exec -it ho_emulator /bin/bash # or docker compose exec -it ho_emulator /bin/bash
# cd ho_emulator && python3 main.py
```
6. 