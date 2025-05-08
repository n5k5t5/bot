import os, sys
import json
import logging
from abc import ABC


SOCKET_FILE = '/usr/local/mtxprc/muni_pricing/tmp/socket_{pid}_{suffix}'
BYTE_ORDER = 'little'
HEADERSIZE = 8
INIT_DATA = 'INIT_DATA'
TASK = 'TASK'

log_format = f'pid {os.getpid()} %(filename)s %(asctime)s %(levelname)s: %(message)s'
datefmt = '%Y%m%d %H:%M:%S'

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format=log_format, datefmt=datefmt)


encode_msg = lambda x: json.dumps(x).encode()


decode_msg = lambda y: json.loads(y.decode())


def send_msg(msg, out_stream):
    logging.debug(f'Sending msg {msg}')
    raw_msg = encode_msg(msg)
    msg_size = len(raw_msg)
    header = int.to_bytes(msg_size, HEADERSIZE, BYTE_ORDER)
    out_stream.write(header)
    out_stream.write(raw_msg)
    out_stream.flush()
    logging.debug('sent')


def read_msg(in_stream):
    logging.debug(f'Reading msg')
    header = in_stream.read(HEADERSIZE)
    logging.debug(f'Read header {header}')
    if not header:
        logging.info(f'{in_stream} is closed')
        return None, None
    elif len(header) < HEADERSIZE:
        raise Exception(f'Reading: header is too small: size {len(header)}')
    msg_size = int.from_bytes(header, BYTE_ORDER)
    logging.debug(f'Reading msg body (size {msg_size})')
    raw_msg = in_stream.read(msg_size)
    logging.debug(f'Read msg body (size {msg_size})')
    msg = decode_msg(raw_msg)
    logging.debug(f'Read {msg}')
    return msg, True


class AbstractWorker(ABC):
    def connect(self):
        return

    def initialize(self, init_data):
        return True
    
    def do_task(self, task):
        pass

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
        except Exception as error:
            res = str(error)
        return ind, res
