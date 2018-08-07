import logging
import os
import sys

from swearch.utils import *  # NOQA


def setup_logger(log_file=None, level='WARNING'):
    log = logging.getLogger()
    log.setLevel(level)

    if log_file:
        log_file = os.path.abspath(log_file)
        log_handler = logging.FileHandler(log_file)
    else:
        log_handler = logging.StreamHandler(sys.stdout)

    log_format = logging.Formatter("%(asctime)s %(levelname)-5s - %(message)s")
    log_handler.setFormatter(log_format)
    log.addHandler(log_handler)
