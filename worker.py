import os
import socket
import logging
from workerpool import common as bp


logging.getLogger().setLevel(logging.INFO)


# This loop is executed by the worker process...
def worker_loop(input, output, worker):
    worker.connect()
    while True:
        try:
            msg, success = bp.read_msg(input)
            if not success:
                return
            kind, content = msg
            if kind == bp.INIT_DATA:
                worker.initialize(content)
                bp.send_msg([kind, True], output)
            elif kind == bp.TASK:
                response = worker.do_task(content)
                bp.send_msg([kind, response], output)
        except Exception as error:
            logging.error(error)


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
                    worker_loop(input, output, worker)
        except Exception as error:
            logging.error(error)
        finally:
            os.unlink(socket_file)