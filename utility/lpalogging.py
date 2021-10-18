"""
Logging module
"""
import logging
import os
import glob
import datetime
from execution.config import *

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Check the names of all log files generated today
date = str(datetime.date.today())
log_file_name_list = glob.glob("{}/{}*".format(LOG_DIR, date))
existed_indice = [-1]
for file_name in log_file_name_list:
    index = int([i.strip() for i in file_name.split("-")][3])
    existed_indice.append(index)

log_index = max(existed_indice) + 1
LOG_FILE_NAME = "{}/{}-{}".format(LOG_DIR, date, log_index)
logging.basicConfig(filename=LOG_FILE_NAME, level=logging.DEBUG)


class LpaLogger(object):
    def __init__(self):
        print("Initiliazing LPA logger......")

    def info(self, info_str):
        if LOG:
            logging.info(info_str)
