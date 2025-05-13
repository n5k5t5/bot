import os
from subprocess import Popen
from threading import Thread, Event, Lock, Condition
from queue import Queue
from time import time, sleep
from datetime import datetime
import logging
from math import ceil
import random
import socket
import selectors
from workerpool import common as bp


MAIN_PROCESS = f'PID {os.getpid()}'
INSTANTIATED = 'INSTANTIATED'
CONNECTED = 'CONNECTED'
READY_FOR_TASKS = 'READY_FOR_TASKS'
KILLED = 'KILLED'
MIN_WORKERS = 5
SANE_WORKERS = os.cpu_count() or 12
MAX_WORKERS = max(25, (3 * SANE_WORKERS) // 2)
SPEED_ADJ = 100
MIN_TIME_STEP = 5


logging.getLogger().setLevel(logging.INFO)


min_sec = lambda sec: f'{sec // 60 :.0f} min {sec % 60 :.0f} s'


def get_socket(socket_file):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(socket_file)
    sf = s.makefile('rwb')
    return s, sf


class Socket():
    def __init__(self, pid, suffix=''):
        '''
        A client socket for connecting to an existing worker
        :param pid: pid of the worker
        '''
        self.pid = pid
        self.name = f'pid {self.pid}'
        self.in_stream = None
        self.out_stream = None
        self.socket_file = bp.SOCKET_FILE.format(pid=self.pid, suffix=suffix)
        self.status = INSTANTIATED

    def attempt_connect(self):
        self.socket, io_stream = get_socket(self.socket_file)
        self.in_stream = io_stream
        self.out_stream = io_stream

    def connect(self):
        while True:
            try:
                self.attempt_connect()
                break
            except:
                sleep(1)
        self.status = CONNECTED

    def send_msg(self, data):
        return bp.send_msg(data, self.out_stream)
    
    def read_msg(self):
        return bp.read_msg(self.in_stream)
    
    def initialize(self, init_data):
        self.send_msg([bp.INIT_DATA, init_data])
        logging.debug(f'Sent init data')
        msg, success = self.read_msg()
        logging.debug('Received init response')
        if success:
            if msg == [bp.INIT_DATA, True]:
                self.status = READY_FOR_TASKS
                logging.info(f'{self.name} is ready for tasks')
                return True
            else:
                logging.error(f'{self.name} failed to set up')
                logging.error(f'{self.name} init response:  {msg}')
                return False
        else:
            logging.info(f'{self.name} is dead')
            return
    
    def send_task(self, data):
        '''
        Sumbits task to the worker
        :param data: [index, content]
        '''
        self.send_msg([bp.TASK, data])

    def get_task_result(self):
        msg, success = self.read_msg()
        assert success
        kind, ret_data = msg
        return ret_data
        
    def do_task(self, data):
        '''
        Submits task to the worker and returns the result.
        :param data: [index, content]
        '''
        self.send_task(data)
        res =  self.get_task_result()
        return res


class Worker(Socket):
    '''
    A socket which results after starting a process by a given command.
    '''
    def __init__(self, command):
        self.command = command
        self.process = Popen(command)
        super().__init__(self.process.pid)
        logging.info(f'Worker launched, command: {command}, '
            f"id = {self.process.pid}, socket file: {self.socket_file}")

    def initialize(self, init_data):
        ret_val = super().initialize(init_data)
        if not ret_val:
            self.status = KILLED
            self.kill()
        return ret_val

    def has_quit(self):
        return self.process.poll() is not None
    
    def kill(self):
        logging.info(f'Removing socket file {self.socket_file}')
        try:
            os.unlink(self.socket_file)
            logging.info(f'Removed {self.socket_file}')
        except:
            pass
        logging.info(f'Killing process {self.process.pid}')
        self.process.kill()
        logging.info(f'Killed {self.process.pid}.')


class Empty(Exception):
    pass


class Node:
    def __init__(self, task, requestor=''):
        self.task = task
        self.pred = None
        self.succ = None
        self.requestor = requestor
        self.rpred = None
        self.rsucc = None
        self.seen = False


class RequestorQueue:
    '''
    Get and the next in line from a specific category or, if none available, just the next in line.
    Delete elements in O(1) time.
    '''
    def __init__(self):
        self.rheads = {}
        self.head = None
        self.rtails = {}
        self.tail = None
        self.qsize = 0

    def __len__(self):
        return self.qsize

    def append(self, new_node):
        requestor = new_node.requestor
        if self.head is None:
            self.head = new_node
        else:     
            self.tail.succ = new_node
            new_node.pred = self.tail
        self.tail = new_node
        if requestor not in self.rtails:
            self.rtails[requestor] = new_node
            self.rheads[requestor] = new_node
        else:
            self.rtails[requestor].rsucc = new_node
            new_node.rpred = self.rtails[requestor]
            self.rtails[requestor] = new_node
        self.qsize += 1

    def __contains__(self, node):
        return (self.tail is node) if node.succ is None else (node.succ.pred is node)

    def discard(self, node):
        if node.succ is None:
            if node.pred is None:
                if self.head is node:
                    self.head = self.tail = None
                else:
                    return
            else:
                assert node.pred.succ is node
                node.pred.succ = None
                self.tail = node.pred
        else:
            assert node.succ.pred is node
            if node.pred is None:
                node.succ.pred = None
                self.head = node.succ
            else:
                assert node.pred.succ is node
                node.pred.succ = node.succ
                node.succ.pred = node.pred
        
        if node.rsucc is None:
            if node.rpred is None:
                del self.rheads[node.requestor]
                del self.rtails[node.requestor]
            else:
                node.rpred.rsucc = None
                self.rtails[node.requestor] = node.rpred
        else:
            if node.rpred is None:
                node.rsucc.rpred = None
                self.rheads[node.requestor] = node.rsucc
            else:
                node.rpred.rsucc = node.rsucc
                node.rsucc.rpred = node.rpred

        self.qsize -= 1
        node.succ = node.pred = node.rsucc = node.rpred = None
     
    def peekleft(self, requestor=''):
        assert self.head is not None
        return self.rheads[requestor] if requestor in self.rheads else self.head

    def popleft(self, requestor=''):
        node = self.peekleft(requestor=requestor)
        self.discard(node)
        return node


class Sprint:
    '''
    Like a jira sprint
    '''
    def __init__(self, init_data=None, with_inds=False):
        self.mutex = Lock()
        self.not_empty = Condition(self.mutex)
        self.init_data = init_data
        self.tasks = RequestorQueue()
        self.num_submitted_tasks = 0
        self.nums_submitted_tasks = {}
        self.nums_picked_res = {}
        self.in_progress = dict()
        self.out_queues = {}
        self.open = True
        self.is_done_event = Event() # the batch is closed and all tasks are completed
        self.result_count = 0
        self.ind_lookup = {} if with_inds else None
        self.requestors = {}
        self.log = Queue()
        self.recycle = False
        self.recycled = set() # for logging only
        self.chunksize = 1
    def submit_tasks(self, tasks, requestor='') -> int:
        '''
        Submit an ask for execution
        :param ask:
        :return: submission sequence number
        '''
        with self.mutex:
            for task in tasks:
                seq_num = self.num_submitted_tasks
                if self.ind_lookup is not None:
                    name, task = task
                    self.ind_lookup[seq_num] = name
                self.requestors[seq_num] = requestor
                task = (seq_num, task)
                self.num_submitted_tasks += 1
                self.nums_submitted_tasks[requestor] = self.nums_submitted_tasks.setdefault(requestor, 0) + 1 
                self.tasks.append(Node(task, requestor=requestor))
            self.not_empty.notify(len(tasks))
            return self.num_submitted_tasks
    
    
    def submit_task(self, task, requestor='') -> int:
        return self.submit_tasks([task], requestor=requestor)
    

    def get_task(self, requestor=''):
        '''
        :return: the next task in the queue so as to execute it
        '''
        with self.mutex:
            while True:
                if len(self.tasks):
                    node = self.tasks.popleft(requestor=requestor)
                    task_to_grab = node.task
                    idx = task_to_grab[0]
                    if not node.seen:
                        # new task, update records
                        self.in_progress[idx] = node
                        node.seen = True
                    if self.recycle:
                        self.tasks.append(node) # let another worker pick it up...
                        self.recycled.add(idx)
                    else:
                        self.recycled.discard(idx)
                    return task_to_grab
                elif self.open:
                    self.not_empty.wait()
                    continue
                else:
                    raise Empty

    def submit_result(self, ind: int, result, worker_name):
        '''
        :param ind: the sequence number of the task whose result is being submitted
        :param result: the result of execution of the task
        '''
        time_now = time()
        with self.mutex:
            if ind in self.in_progress:
                self.tasks.discard(self.in_progress[ind])
                del self.in_progress[ind]
                requestor = self.requestors[ind]
                self.out_queues.setdefault(requestor, Queue()).put((ind, result))
                self.result_count += 1
            # To log arrival stats...
            self.log.put([ind, time_now, worker_name])
            if not self.open and self.num_submitted_tasks == self.result_count:
                self.is_done_event.set()

    def yield_results(self, requestor=''):
        '''
        Not to be called by multiple threads
        :param requestor: the  name of the requestor
        '''
        if requestor not in self.nums_picked_res:
            self.nums_picked_res[requestor] = 0
        out_queue = self.out_queues.setdefault(requestor, Queue())
        logging.debug(f'started iterating over results submitted by "{requestor}"...')
        while self.nums_picked_res[requestor] < self.nums_submitted_tasks[requestor]:
            resp = out_queue.get()
            idx = resp[0]
            if self.ind_lookup is not None:
                resp = (self.ind_lookup.pop(idx), resp[1])
            self.requestors.pop(idx)
            self.nums_picked_res[requestor] += 1
            yield resp


    def __iter__(self):
        '''
        for backward compatibility
        '''
        return self.yield_results()

    def get_results(self, requestor='') -> list:
        '''
        For backward compatibility only
        Get the remaining results in a list with 1-1 correspondence to the tasks
        '''
        result_pairs = list(self.yield_results(requestor=requestor))
        results = [None] * (1 + max(item[0] for item in result_pairs))
        for x, y in result_pairs:
            results[x] = y
        return results

    def is_done(self):
        return self.is_done_event.is_set()
    
    def close(self):
        '''
        Once the batch is closed, it is not to accept any more asks but can continue to
        work on already submitted ones.
        '''

        if self.open:
            self.open = False
            self.start_recycle()

    def start_recycle(self):
        count = 0
        with self.mutex:
            self.recycle = True
            for idx, node in self.in_progress.items():
                if node not in self.tasks:
                    self.tasks.append(node)
                    self.recycled.add(idx)
                    count += 1
            self.not_empty.notify(count)


class SprintMan:
    '''
    A worker equipped with a queue for sprints to work on and a thread for working on them.
    '''
    def __init__(self, worker, requestor=''):
        self.sprints = Queue()
        self.actual = worker
        self.thread = Thread(target=self.worker_loop, daemon=True)
        self.sprint_status = (None, INSTANTIATED)
        self.stop_order = False
        self.requestor = requestor

    def work_on_sprint(self):
        sprint = self.sprint_status[0]
        assert self.actual.initialize(sprint.init_data)
        self.sprint_status = (sprint, READY_FOR_TASKS)
        while True:
            if self.stop_order:
                self.stop_order = False
                break
            try:
                task = sprint.get_task(requestor=self.requestor)
            except Empty:
                break
            try:
                ind = task[0]
                ret_ind, res = self.actual.do_task(task)
                assert ind == ret_ind
            except Exception as error:
                logging.warning(f'{self.actual.name} failed to process task # {ind}')
                logging.warning(f'with error: {error}')
                if self.actual.has_quit():
                    logging.info(f'{self.actual.name} has quit')
                    return                  
            else:
                sprint.submit_result(ind, res, self.actual.name)

    def worker_loop(self):
        self.actual.connect()
        while True:
            self.sprint_status = (None, CONNECTED)
            sprint = self.sprints.get()
            if sprint == 'return':
                break
            self.sprint_status = (sprint, CONNECTED)
            try:
                self.work_on_sprint()
            except Exception as error:
                logging.error(error)
                return


class Batch(Sprint):
    def __init__(self, tasks, init_data=None, shuffle=False):
        super().__init__(init_data=init_data, with_inds=shuffle)
        if shuffle:
            # Randomize the order of tasks to better estimate speed of processing...
            tasks = random.sample(list(enumerate(tasks)), len(tasks))
        self.submit_tasks(tasks)
        self.close()


class Swarm:
   def __init__(self, command, num_workers=None, task_processor=None, initializer=None):
        if num_workers is None:
            num_workers = SANE_WORKERS
        self.command = command
        logging.info(f'Worker command: {self.command}')
        self.workers = [] # it is useful to retain the order in which workers were created...
        if task_processor:
            self.house_worker = SprintMan(bp.HouseWorker(task_processor, initializer=initializer))
            self.house_worker.thread.start()
        else:
            self.house_worker = None
        self.add_workers(num_workers)
        self.sprint_to_workers = dict()

class Pool:
    '''
    A pool of workers
    '''
    def __init__(self, command, num_workers=None, task_processor=None, initializer=None):
        if num_workers is None:
            num_workers = SANE_WORKERS
        self.command = command
        logging.info(f'Worker command: {self.command}')
        self.workers = [] # it is useful to retain the order in which workers were created...
        if task_processor:
            self.house_worker = SprintMan(bp.HouseWorker(task_processor, initializer=initializer))
            self.house_worker.thread.start()
        else:
            self.house_worker = None
        self.add_workers(num_workers)
        self.sprint_to_workers = dict()
        
    def assign_worker_to_sprint(self, worker, batch):
        # TODO 
        self.sprint_to_workers.setdefault(batch, set()).add(worker)
        worker.sprints.put(batch)

    def add_workers(self, num_workers):
        added_workers = set()
        while len(added_workers) < num_workers:
            command = self.command
            try:
                worker = SprintMan(Worker(command))
            except Exception as error:
                logging.warning(f'Failed to launch a worker.  Command: {command}')
                logging.error(error)
                continue      
            worker.thread.start()        
            self.workers.append(worker)
            logging.info(f'Appended worker {worker.actual.name} to the worker list')
            added_workers.add(worker)      
        return added_workers
    
    def add_workers_to_sprint(self, num_workers, batch):
        '''
        Adds existent workers to the batch, and if that is not enough, spin up some new workers
        '''
        workers_to_add = set()
        batch_workers = self.sprint_to_workers.setdefault(batch, set())
        for worker in self.workers:
            if len(workers_to_add) == num_workers:
                break
            if worker not in batch_workers:
                workers_to_add.add(worker)
        workers_to_add.update(self.add_workers(num_workers - len(workers_to_add)))
        for worker in workers_to_add:
            self.assign_worker_to_sprint(worker, batch)

    def submit_sprint(self, batch, num_workers=SANE_WORKERS):
        '''
        '''
        self.add_workers_to_sprint(num_workers, batch)
        if self.house_worker:
            self.house_worker.sprints.put(batch)

    def submit_batch(self, batch, num_workers=SANE_WORKERS):
        '''
        Synonym to submit_sprint, for back-compatibility
        '''
        return self.submit_sprint(batch, num_workers)

    def remove_dead_workers(self, batch):
        alive_workers = []
        for worker in self.workers:
            if worker.actual.has_quit():
                self.sprint_to_workers[batch].discard(worker)
            else:
                alive_workers.append(worker)
        self.workers = alive_workers
            
    def close(self):
        for w in self.workers:
            w.actual.kill()
        logging.info('Worker Pool is closed')          

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, tb):
        self.close()


def print_contributions(pool, batch, contributions):
    is_done = batch.is_done()
    while not batch.log.empty():
        worker_name = batch.log.get()[2]
        contributions[worker_name] = contributions.get(worker_name, 0) + 1
    logging.info('Worker Contributions by task counts and percent:')
    worker_stati = {pool.house_worker.actual.name: None} if pool.house_worker else {}
    for worker in pool.workers:
        worker_stati[worker.actual.name] = worker.actual.status

    for worker_name, status in worker_stati.items():
        contribution = contributions.get(worker_name, 0)
        if is_done:
            logging.info(f'Worker: {worker_name}, status: {status}, contrib: {contribution}, {round(contribution / batch.num_submitted_tasks * 100, 1)}%')
        else:
            logging.info(f'Worker: {worker_name}, status: {status}, contrib: {contribution}')
    if is_done:
        logging.info(f'Total contributions = {sum(contributions.values())}, total tasks = {batch.num_submitted_tasks}')


class BatchManager:
    '''
    This class deploys new workers if and when calculation runs late.
    '''
    @staticmethod
    def trim_workers(num_workers):
        num_workers = min(num_workers, MAX_WORKERS)
        num_workers = max(num_workers, MIN_WORKERS)
        return num_workers
    
    @staticmethod
    def send_progress_update(speed, avg_speed, time_remains, tasks_remain, num_submitted_tasks):
        desired_speed = tasks_remain / time_remains
        speed_ = speed * 60
        avg_speed_ = avg_speed * 60
        desired_speed_ = desired_speed * 60
        # Based on current speed...
        exp_delay = tasks_remain / speed - time_remains
        # Based on average speed...
        overall_exp_delay = tasks_remain / avg_speed - time_remains
        delays_str = f'{exp_delay / 60 :.2f} / {overall_exp_delay / 60:.2f} min'
        eta_str = min_sec(tasks_remain / speed)
        arrival_update = f'(Current/Avg/Desired) speeds: {speed_} / {avg_speed_} / {desired_speed_}, delays: {delays_str}, remains {tasks_remain} / {num_submitted_tasks}, should be finished in {eta_str}'
        logging.info(arrival_update)

    def __init__(self, pool, asks, init_data=None):
        self.active_workers = set()
        self.pending_workers = set()
        self.contributions = {}
        self.start_time = None
        self.tta = None
        self.main_thread = None
        self.batch = Batch(asks, init_data=init_data, shuffle=True)
        self.pool = pool

    def survey_workers(self):
        active_workers = set()
        pending_workers = set()
        self.pool.remove_dead_workers(self.batch)
        for worker in self.pool.sprint_to_workers[self.batch]:
            if worker.sprint_status == (self.batch, READY_FOR_TASKS):
                active_workers.add(worker)
            else:
                pending_workers.add(worker)
        self.active_workers = active_workers
        self.pending_workers = pending_workers
 
    def set_workers(self, num_workers):
        self.survey_workers()
        logging.info(f'Workers: current total: {len(self.pool.workers)}, active: {len(self.active_workers)}, pending: {len(self.pending_workers)}, '
            f'desired: {num_workers}')
        num_workers_to_add = max(0, num_workers - len(self.active_workers) - len(self.pending_workers))
        logging.info(f'Adding {num_workers_to_add} workers')
        self.pool.add_workers_to_sprint(num_workers_to_add, self.batch)
        self.survey_workers() # to update the sets accessed in the next line...  
        logging.info(f'Workers: current total: {len(self.pool.workers)}, active: {len(self.active_workers)}, pending: {len(self.pending_workers)}')

    def batch_manager(self, num_workers=None, tasks_per_worker=None, tta=None, speed_adj=SPEED_ADJ):
        self.start_time, self.tta = time(), tta
        batch = self.batch
        try:            
            if not batch.num_submitted_tasks:
                batch.is_done_event.set()
                return
            num_workers = num_workers or (batch.num_submitted_tasks // tasks_per_worker if tasks_per_worker else SANE_WORKERS)
            num_workers = BatchManager.trim_workers(num_workers)
            logging.info(f'Starting with  {batch.num_submitted_tasks} tasks and {num_workers} workers.')
            self.pool.submit_batch(batch, num_workers)
            if speed_adj and tta is not None:
                logging.info(f'Target time of completion = {datetime.fromtimestamp(tta)}')
                speed_adj = min(speed_adj, len(batch.tasks))
                self.time_step = (self.tta - self.start_time) // (speed_adj or 1)
                self.time_step = max(self.time_step, MIN_TIME_STEP)
                # Start a progress-monitoring thread...
                self.speed_adjustment(gradual=False)
            batch.is_done_event.wait()
            logging.info(f'Done.  Target time of completion: {datetime.fromtimestamp(self.tta) if self.tta else None}.')
            print_contributions(self.pool, self.batch, self.contributions)
        finally:
            logging.info('batch_manager thread exited')

    def start(self, *args, **kwargs):
        self.main_thread = Thread(target=self.batch_manager, args=args, kwargs=kwargs, daemon=True)
        self.main_thread.start()

    def speed_adjustment(self, gradual=True):
        full_throttle = False
        prev_time = time()
        batch = self.batch
        prev_result_count = batch.result_count
        while True:
            # Wait for some new results to come in so we could estimate speed
            # while frequently checking for the done event...
            sleep(1)
            if batch.is_done():
                return
            if batch.result_count == prev_result_count or time() - prev_time <= self.time_step:
                continue
            self.survey_workers()
            num_active_workers = len(self.active_workers)
            time_now = time()
            result_count = batch.result_count
            elapsed_time = time_now - self.start_time
            time_remains = self.tta - time_now
            time_delta = time_now - prev_time
            tasks_remain = batch.num_submitted_tasks - result_count
            task_delta = result_count - prev_result_count
            speed = task_delta / time_delta
            avg_speed = result_count / elapsed_time
            desired_speed = tasks_remain / time_remains
            BatchManager.send_progress_update(speed, avg_speed, time_remains, tasks_remain, batch.num_submitted_tasks)
            desired_num_workers = num_active_workers
            if not full_throttle:
                full_throttle = time_remains <= 0
                if full_throttle:
                    desired_num_workers = BatchManager.trim_workers(num_active_workers + len(batch.tasks))
                    logging.info(f'Missed the target, blasting full throttle...')
                    self.set_workers(desired_num_workers)
                else:
                    if speed < desired_speed and avg_speed < desired_speed:
                        # it is time to get aggressive...
                        logging.info('We need to speed up...')
                        if gradual:
                            desired_num_workers = num_active_workers + 1 
                        else:
                            desired_num_workers = ceil((num_active_workers + 1) * desired_speed / speed) - 1
                            # ...to account for the house worker
                        desired_num_workers = BatchManager.trim_workers(desired_num_workers)
                        self.set_workers(desired_num_workers)
            logging.info(f'Numbers of workers: currently active: {num_active_workers}, desired: {desired_num_workers}')
            prev_time = time_now
            prev_result_count = result_count

    def close(self):
        if self.main_thread:
            self.main_thread.join()
