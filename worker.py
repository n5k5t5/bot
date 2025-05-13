import os
import socket
import logging
from time import time
from workerpool import common as bp
import __main__


logging.getLogger().setLevel(logging.INFO)
try:
    rootname = os.path.splitext(os.path.basename(__main__.__file__))[0]
except:
    rootname = ''
LOGFREQ = 20


# This loop is executed by the worker process...
def worker_loop(input, output, worker):
    worker.connect()
    counter = total_time = read_time = calc_time = send_time = msg_size = 0
    while True:
        try:
            when_start = time()
            msg, success = bp.read_msg(input)
            when_read = time()
            if not success:
                return
            kind, content = msg
            if kind == bp.INIT_DATA:
                worker.initialize(content)
                response = True
            elif kind == bp.TASK:
                response = worker.do_task(content)
            when_calced = time()
            bp.send_msg([kind, response], output)
            when_done = time()
            total_time += when_done - when_start
            read_time += when_read - when_start
            calc_time += when_calced - when_read
            send_time += when_done - when_calced
            msg_size += success
            counter += 1
            if counter % LOGFREQ == 0:
                logging.info(f'{rootname}: size total read calc send (per call): '
                    f'{msg_size / LOGFREQ} '
                    f'{total_time / LOGFREQ * 1000} ms '
                    f'{round(read_time / total_time * 100, 2)}% '
                    f'{round(calc_time / total_time * 100, 2)}% '
                    f'{round(send_time / total_time * 100, 2)}% '
                    )
                total_time = read_time = calc_time = send_time = msg_size = 0
        except Exception as error:
            logging.error(error)


def run_socket(loop, suffix=''):
    '''
    :param loop: a function taking two arguments: input: readable binary stream, output, writable binary stream
    :param suffix: suffix for forming socket name
    '''
    socket_file = bp.SOCKET_FILE.format(pid=os.getpid(), suffix=suffix)
    try:
        os.unlink(socket_file)
    except OSError:
        if os.path.exists(socket_file):
            raise
    os.makedirs(os.path.dirname(socket_file), exist_ok=True)
    
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        try:
            s.bind(socket_file)
            s.listen(1)
            logging.info(f'Listening on file {socket_file}')
            while True:
                conn, addr = s.accept()
                with conn:
                    logging.info(f'Connected by {addr}')
                    input = output = conn.makefile('rwb')
                    loop(input, output)
        except Exception as error:
            logging.error(error)
        finally:
            os.unlink(socket_file)


# Use this to create a task-processing server to be used as a worker...
def run(worker=None, task_processor=None, initializer=None, suffix=''):
    '''
    :param worker: 
        example:
            worker.initialize(INIT_DATA)
            result = worker.do_task(task)
    '''
    if not worker:
        worker = bp.HouseWorker(task_processor, initializer=initializer)
    loop = lambda i, o: worker_loop(i, o, worker)
    run_socket(loop, suffix=suffix)
