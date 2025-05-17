import os
from abc import ABC
from pickle import loads as loadb, dumps as dumpb
from datetime import datetime
import __main__


SOCKET_FILE = '/var/run/socket_{pid}_{suffix}'
BYTE_ORDER = 'little'
INTSIZE = 8
INIT_DATA = b'initialize'
TASK = b'do_task'


encode_int = lambda x: int.to_bytes(x, INTSIZE, BYTE_ORDER)
decode_int = lambda y: int.from_bytes(y[:INTSIZE], BYTE_ORDER)


def send_msg(raw_msg: bytes, out_stream):
    out_stream.write(encode_int(len(raw_msg)) + raw_msg)
    out_stream.flush()


def read_msg(in_stream):
    header = in_stream.read(INTSIZE)
    if not header:
        logger.info(f'{in_stream} is closed')
        return None, None
    elif len(header) < INTSIZE:
        raise Exception(f'Reading: header is too small: size {len(header)}')
    msg_size = decode_int(header)
    raw_msg = in_stream.read(msg_size)
    return raw_msg, INTSIZE + msg_size  # or just anything that has a boolean value of True


def decompose_msg(msg: bytes):
    target_len = decode_int(msg)
    # target, raw_idx, arg
    return msg[INTSIZE: INTSIZE + target_len], msg[INTSIZE + target_len: 2 * INTSIZE + target_len], msg[2 * INTSIZE + target_len:]


def compose_msg(target: bytes, raw_idx: bytes, arg: bytes):
    return encode_int(len(target)) + target + raw_idx + arg


class Logger:
    try:
        root_name = os.path.splitext(os.path.basename(__main__.__file__))[0]
    except:
        root_name = ''
    
    def __init__(self, module_path):
        self.module_name = os.path.splitext(os.path.basename(module_path))[0]
        self.error = self._vocal('ERROR')
        self.warning = self._vocal('WARNING')
        self.info = self._vocal('INFO')
        self.debug = self._silent

    def _silent(self, *args, **kwargs): 
        return
    
    def _vocal(self, mode):
        def f(*args, **kwargs):
            return print(f'{self.root_name} {self.module_name} {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} {os.getpid()} {mode}:',  *args, **kwargs)
        return f
 

class AbstractWorker(ABC):
    def connect(self): ...

    def initialize(self, init_data): ...
    
    def do_task(self, task): ...

    def has_quit(self) -> bool: ...


class HouseWorker(AbstractWorker):
    def __init__(self, calls, name='House Worker'):
        '''
        :param task_processor: a function like object taking one argument and not taking exceptions
        : param initializer: optional, for backwards
        '''
        self.calls = calls
        self.name = name

    def initialize(self, init_data):
        return self.calls[INIT_DATA](init_data)
        
    def do_task(self, task):
        return task[0], self.calls[TASK](task[1])
    
    def execute(self, msg):
        target, raw_idx, arg = decompose_msg(msg)
        func = self.calls[target]
        return compose_msg(target, raw_idx, dumpb(func(loadb(arg))))

    def has_quit(self):
        return False
    
    
logger = Logger(__file__)
