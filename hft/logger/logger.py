import logging
import os
from logging import handlers
import time

def get_logger(log_path, file_name=None):
    file_name = "{}_{}.log".format(file_name, time.strftime('%Y_%m_%d_%H_%M_%S'))
    logger = logging.getLogger(file_name)

    if not os.path.exists(log_path):
        os.makedirs(log_path)
    file_name = os.path.join(log_path, file_name)

    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s# %(message)s")

    fh = handlers.TimedRotatingFileHandler(filename=file_name, when='H', encoding='utf-8', backupCount=0)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.setLevel(logging.INFO)
    return logger