import os, sys
import logging
from abc import ABC
from pickle import loads, dumps
import traceback
import __main__


SOCKET_FILE = '/usr/local/mtxprc/muni_pricing/tmp/socket_{pid}_{suffix}'
BYTE_ORDER = 'little'
INTSIZE = 8
INIT_DATA = 'INIT_DATA'
TASK = 'TASK'
log_format = f'pid {os.getpid()} %(filename)s %(asctime)s %(levelname)s: %(message)s'
datefmt = '%Y%m%d %H:%M:%S'

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format=log_format, datefmt=datefmt)


encode_msg = dumps
decode_msg = loads


def send_msg(msg, out_stream):
    raw_msg = encode_msg(msg)
    msg_size = len(raw_msg)
    header = int.to_bytes(msg_size, INTSIZE, BYTE_ORDER)
    out_stream.write(header + raw_msg)
    out_stream.flush()


def read_msg(in_stream):
    header = in_stream.read(INTSIZE)
    if not header:
        logging.info(f'{in_stream} is closed')
        return None, None
    elif len(header) < INTSIZE:
        raise Exception(f'Reading: header is too small: size {len(header)}')
    msg_size = int.from_bytes(header, BYTE_ORDER)
    raw_msg = in_stream.read(msg_size)
    msg = decode_msg(raw_msg)
    return msg, INTSIZE + msg_size  # or just anything that has a boolean value of True


def parse_msg(msg):
    header_size = int.from_bytes(msg[:INTSIZE], BYTE_ORDER)
    return msg[INTSIZE, INTSIZE + header_size], msg[INTSIZE + header_size: 2 * INTSIZE + header_size], msg[2 * INTSIZE + header_size:]


def compose_msg(target, idx, arg):
    return int.to_bytes(len(target), INTSIZE, BYTE_ORDER) + target + int.to_bytes(idx, INTSIZE, BYTE_ORDER) + arg


class AbstractWorker(ABC):
    def connect(self): ...

    def initialize(self, init_data):
        return True
    
    def do_task(self, task): ...

    def has_quit(self):
        return False

class HouseWorker(AbstractWorker):
    def __init__(self, task_processor, initializer=None, name='House Worker'):
        self.task_processor = task_processor
        self.initializer = initializer
        self.name = name

    def initialize(self, init_data):
        if init_data is not None:
            self.initializer(*init_data)
        return True
    
    def do_task(self, task):
        ind, ask = task
        try:
            res = self.task_processor(ask)
        except Exception:
            res = traceback.format_exc()
        return ind, res
