#!/usr/bin/env python3

import subprocess
import os
import sys
import time
import datetime
import argparse
import atexit

f = open("./waf_log.txt", 'w')

def at_exit_func():
    print("total user writes: ", total_user_write)
    print("total_nand_write: ", total_nand_write)
    f.close()

atexit.register(at_exit_func)

nvme = "nvme"
address = "/dev/nvme0"

extended_smart_gen=[
[264, 279, 16, 'Lifetime user writes'],
[280, 295, 16, 'Lifetime NAND writes']]

extended_smart = extended_smart_gen
log_id = "0xCA"

cmd="sudo {0} get-log {1} --log-id={2} --log-len=1024 -b".format(nvme, address, log_id)
out = subprocess.check_output(cmd, shell=True)

i = extended_smart[0]
total_user_write = int.from_bytes(out[i[0]:i[1]], byteorder='little')

i = extended_smart[1]
total_nand_write = int.from_bytes(out[i[0]:i[1]], byteorder='little')


while True:
    cmd="sudo {0} get-log {1} --log-id={2} --log-len=1024 -b".format(nvme, address, log_id)
    out = subprocess.check_output(cmd, shell=True)
    
    i = extended_smart[0]
    user_write = int.from_bytes(out[i[0]:i[1]], byteorder='little')
    
    i = extended_smart[1]
    nand_write = int.from_bytes(out[i[0]:i[1]], byteorder='little')
    
    #print((user_write - total_user_write), (nand_write - total_nand_write))

    total_user_write = user_write
    total_nand_write = nand_write

    print(total_user_write, end = ' ')
    print(total_nand_write, end = ' ')
    print(nand_write/user_write, end = '\n')

    f.write(str(total_user_write) + " ")
    f.write(str(total_nand_write) + " ")
    f.write(str(nand_write/user_write) + "\n")

    time.sleep(1)

