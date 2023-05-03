'''
Python package for running interactive workers
'''
from sys import stdin, stdout
import os
import pickle
from collections import deque
from subprocess import Popen, PIPE
from threading import Thread, Event
from queue import Queue
from time import time


HEADERSIZE = 64
BYTE_ORDER = 'little'
PREP = 'prep'
QUERY = 'query'
MAX_WORKERS = 100


class Done(Exception):
	pass


def encode_msg(msg):
	raw_msg = pickle.dumps(msg)
	return raw_msg


def decode_msg(raw_msg):
	msg = pickle.loads(raw_msg)
	return msg


def send_msg(msg, out_stream, logstream=None):
	def write_log(msg):
		print(os.getpid(), ':', msg, file=logstream) if logstream else None

	raw_msg = encode_msg(msg)
	msg_size = len(raw_msg)
	write_log(f'sending: msg = {msg}, msg_size = {msg_size}')
	header = int.to_bytes(msg_size, HEADERSIZE, BYTE_ORDER)
	out_stream.write(header)
	write_log(f'wrote header of len {len(header)}')
	out_stream.write(raw_msg)
	out_stream.flush()
	write_log(f'wrote msg of len {len(raw_msg)}:')
	write_log(f'raw msg={raw_msg}')


def read_msg(in_stream, logstream=None):
	def write_log(msg):
		print(os.getpid(), ':', msg, file=logstream) if logstream else None

	write_log('reading header...')
	header = in_stream.read(HEADERSIZE)
	write_log(f'read header of len {len(header)}...')
	if 0 <  len(header) < HEADERSIZE:
		write_log(f'reading: header is too small: size {len(header)}')
		raise Exception(f'reading: header is too small: size {len(header)}')
	elif not header:
		return None, None
	msg_size = int.from_bytes(header, BYTE_ORDER)
	write_log(f'expecting msg size = {msg_size}')
	raw_msg = in_stream.read(msg_size)
	write_log(f'reading: size of raw msg = {len(raw_msg)}')
	write_log(f'raw msg={raw_msg}')
	msg = decode_msg(raw_msg)
	write_log(f'decoded msg = {msg}')
	return msg, True


def start_bot(processor, preprocessor=None, logstream=None):
	def write_log(msg):
		print(os.getpid(), ':', msg, file=logstream) if logstream else None

	while True:
		try:
			msg, success = read_msg(stdin.buffer, logstream=logstream)
			if not success:
				write_log('eof')
				return
			kind, content = msg
			if kind == PREP:
				if preprocessor:
					params = preprocessor(content)
				send_msg([kind, True], stdout.buffer, logstream=logstream)
			elif kind == QUERY:
				index, data = content
				result = processor(data, params) if preprocessor else processor(data)
				response = [index, result]
				send_msg([kind, response], stdout.buffer, logstream=logstream)
		except:
			pass


def man_bot(worker, param, in_queue, out_queue, logstream=None):
	def write_log(msg):
		print(os.getpid(), ':', msg, file=logstream) if logstream else None

	process = worker.process
	print(os.getpid(), ':', f'Handling process {process.pid}...')

	send_msg([PREP, param], process.stdin, logstream=logstream)
	msg, success = read_msg(process.stdout, logstream=logstream)
	if not success:
		write_log(f'pid {process.pid} is dead')
	else:
		if msg == [PREP, True]:
			worker.greenlight.set()
				
	while worker.greenlight.wait():
		try:
			item = in_queue.popleft()
		except IndexError:
			worker.pause() # because no tasks left
			continue
		write_log(f'got job {item[0]} from the queue')

		try:
			send_msg([QUERY, item], process.stdin, logstream=logstream)
			msg, success = read_msg(process.stdout, logstream=logstream)
			assert success
			kind, result = msg
		except Exception as ex:
			in_queue.appendleft(item)
			if process.poll() is not None:
				print(os.getpid(), ':', f'the process {process.pid} has quit')
				return
		else:
			out_queue.put([result, time()])
			write_log(f'task {result[0]} is done')


class Worker:
	def __init__(self):
		self.process = None
		self.status = None
		self.greenlight = Event()

	def pause(self):
		self.greenlight.clear()
		
	def resume(self):
		self.greenlight.set()


class Pool:
	def __init__(self, command, tasks, setup=None, logstream=None):
		self.command = command
		self.tasks = deque(enumerate(tasks))
		self.out_queue = Queue()
		self.workers = []
		self.threads = []
		self.setup = setup
		self.logstream = logstream

	def add_workers(self, num_workers, logstream=None):
		logstream = logstream or self.logstream
		for _ in range(num_workers):
			worker = Worker()
			worker.process = Popen(self.command, stdin=PIPE, stdout=PIPE, text=False)
			print(os.getpid(), ':', f'Launched process with pid = {worker.process.pid}')
			thread = Thread(target=man_bot, args=[worker, self.setup, self.tasks, self.out_queue], kwargs={'logstream': logstream}, daemon=True)
			thread.start()
			self.workers.append(worker)
			self.threads.append(thread)
		for _ in range(num_workers):
			for worker in self.workers[len(self.workers) - num_workers:]:
				worker.greenlight.wait()

	def close(self):
		for w in self.workers:
			w.process.kill()
			w.process.wait()
			print(f'{w.process.pid} exited with {w.process.poll()}')


class TimelyPool(Pool):
	def __init__(self, command, tasks, setup=None, logstream=None):
		super().__init__(command, tasks, setup=setup, logstream=logstream)
		self.results = Queue()
		self.active_workers = []
		self.sleeping_workers = []
		self.original_load = len(self.tasks)	
		self.result_count = 0

	def _fetch_result(self, block=True):
		self.results.put(self.out_queue.get(block=block))
		self.result_count += 1

	def add_workers(self, deficit):
		if deficit > 0:
			to_awaken = min(deficit, len(self.sleeping_workers))
			for _ in range(to_awaken):
				worker = self.sleeping_workers.pop()
				worker.resume()
				self.active_workers.append(worker)
			if deficit > to_awaken:
				super().add_workers(deficit - to_awaken)
				self.active_workers.extend(self.workers[to_awaken - deficit:])
		else:
			for _ in range(-deficit):
				worker = self.active_workers.pop()
				worker.pause()
				self.sleeping_workers.append(worker)
		print(f'num workers = {len(self.active_workers)}')

	def set_workers(self, num_workers):
		self.add_workers(num_workers - len(self.active_workers))

	def run(self, num_workers=1, tta=None, speed_adj=100):
		self.start_time = time()
		self.tta = tta
		self.sample_size = (self.original_load // speed_adj) if speed_adj else self.original_load

		self.add_workers(num_workers)
		if speed_adj and tta:
			self.adjust_speed()
		while self.result_count < self.original_load:
			self._fetch_result()
		print(f'target runtime {self.tta - self.start_time}, actual: {time() - self.start_time}')

	def adjust_speed(self):
		while self.tasks:
			try:
				target_speed = self.compute_target_speed()
				if not target_speed:
					break
				num_workers = len(self.active_workers)
				# to prevent a potential bias: any slowdown from addition of a worker will
				# propagate to the -1 case at least as much as to +1.
				self.add_workers(1)
				speeds = {}
				for new_num_workers in range(max(1, num_workers-1), min(num_workers + 1, MAX_WORKERS) + 1):
					self.set_workers(new_num_workers)
					speeds[new_num_workers] = self.measure_speed()
				optimum = min(speeds, key=lambda x: abs(speeds[x] - target_speed))
				self.set_workers(optimum)
			except Done:
				pass

	def measure_speed(self):
		tasks_done, elapsed_time = self.partial_run(self.sample_size)
		if not tasks_done:
			raise Done
		speed = tasks_done / elapsed_time if elapsed_time else float('inf')
		print(f'measured speed = {speed}')
		return speed
	
	def start(self, *args, **kwargs):
		Thread(target=self.run, args=args, kwargs=kwargs).start()
		return self
	
	def result_generator(self):
		'''
		Get results in the order they arrived, each in the form ((task_index, result), timestamp)
		'''
		for _ in range(self.original_load):
			yield self.results.get()

	def get_results(self):
		'''
		Get results in a list with 1-1 correspondence to the tasks
		'''
		results = [None] * self.original_load
		for (i, result), _ in self.result_generator():
			results[i] = result
		return results

	def is_done(self):
		return self.result_count == self.original_load
	
	def flush_results(self):
		while True:
			try:
				self._fetch_result(block=False)
			except:
				break

	def partial_run(self, num_tasks=float('inf')):
		self.flush_results()
		tasks_to_do = min(num_tasks, self.original_load - self.result_count)
		elapsed_time  = 0
		if tasks_to_do:
			start_time = time()
			for _ in range(tasks_to_do):
				self._fetch_result()
			elapsed_time = time() - start_time
			speed = tasks_to_do / elapsed_time if elapsed_time else float("inf")
		return tasks_to_do, elapsed_time

	def compute_target_speed(self):
		time_remains = max(self.tta - time(), 1)
		tasks_remain = self.original_load - self.out_queue.qsize()
		target_speed = tasks_remain / time_remains if tasks_remain else None
		print(f'target speed = {target_speed}')
		return target_speed
