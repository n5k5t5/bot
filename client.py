from threading import Lock, Condition, Thread
from random import sample
from collections import deque
from queue import Queue
from abc import ABC, abstractmethod


class Empty(Exception):
    pass


class Sprint:
    def __init__(self, training_data=None):
        self.mutex = Lock()
        self.not_empty = Condition(self.mutex)
        self.training_data = training_data
        self.tasks = deque()
        self.num_submitted_tasks = 0
        self.num_picked_tasks = 0
        self.in_progress = dict()
        self.out_queue = Queue()
        self.open = True
        self.done = False

    def submit_task(self, ask) -> int:
        '''
        Submit an ask for execution
        :param ask:
        :return: submission sequence number
        '''
        with self.mutex:
            task = (self.num_submitted_tasks, ask)
            self.num_submitted_tasks += 1
            self.tasks.append(task)
            self.not_empty.notify()
            return self.num_submitted_tasks
        
    def get_task(self):
        '''
        :return: the next task in the queue so as to execute it
        '''
        while True:
            with self.mutex:
                if len(self.tasks):
                    task = self.tasks.popleft()
                    if task[0] < self.num_picked_tasks:
                        # this task has been picked before
                        if task[0] not in self.in_progress: 
                            continue # task is complete
                    else:
                        # new task
                        assert task[0] == self.num_picked_tasks
                        self.num_picked_tasks += 1
                        self.in_progress[task[0]] = task[1]
                    if not self.open:
                        self.tasks.append(task) # put back just in case...
                    return task
                elif self.open:
                    self.not_empty.wait()
                    continue
                else:
                    raise Empty

    def submit_result(self, ind: int, result):
        '''
        :param ind: the sequence number of the task whose result is being submitted
        :param result: the result of execution of the task
        '''
        with self.mutex:
            if ind in self.in_progress:
                del self.in_progress[ind]
                self.out_queue.put((ind, result))

    def __iter__(self):
        num_yielded_tasks = 0
        while True:
            with self.mutex:
                if num_yielded_tasks == self.num_submitted_tasks:
                    if self.open:
                        self.not_empty.wait()
                    else:
                        self.done = True
                        return
            resp = self.out_queue.get()
            num_yielded_tasks += 1
            yield resp

    def close(self):
        '''
        Once the batch is closed, it is not to accept any more asks but can continue to
        work on already submitted ones.
        '''
        with self.mutex:
            if not self.open:
                return
            self.open = False
            for task in self.in_progress.items():
                self.tasks.append(task)
            self.not_empty.notify_all()


class SprintWorker(ABC):
    def __init__(self):
        self.sprints = Queue()
        self.thread = Thread(self.worker_loop)
        self.sprint_status = (None, 0)

    @abstractmethod
    def initiate(self):
        return True

    @abstractmethod
    def train(self, training_data):
        return True 
    
    @abstractmethod
    def __call__(self, ind, ask):
        return ind, True

    def work_on_sprint(self, log=print):
        sprint = self.sprint_status[0]
        self.train(sprint.training_data)
        self.sprint_status = (sprint, 2)
        while True:
            try:
                ind, ask = sprint.get_task()
            except Empty:
                break
            try:
                ret_ind, res = self(ind, ask)
                assert ret_ind == ind
            except Exception:
                log(f'Failed to do task {ind}')
            else:
                sprint.submit_result(ind, res)

    def worker_loop(self, log=print):
        self.initiate()
        while True:
            self.sprint_status = (None, 1)
            sprint = self.sprints.get()
            self.sprint_status = (sprint, 1)
            self.work_on_sprint(log)



