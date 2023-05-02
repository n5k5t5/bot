'''
Python package for running workers
'''
from sys import stdin, stdout
import os
import pickle
from collections import deque
from subprocess import Popen, PIPE
from threading import Thread, Event
from time import time
from queue import Queue
from math import ceil


HEADERSIZE = 64
BYTE_ORDER = 'little'
PREP = 'prep'
QUERY = 'query'
MAX_WORKERS = 100


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
	def __init__(self, command, tasks, setup=None, logstream=None, tta=None):
		super().__init__(command, tasks, setup=setup, logstream=logstream)
		self.tta = tta
		self.results = deque()
		self.active_workers = []
		self.sleeping_workers = []
		self.original_load = len(self.tasks)	
		
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

	def run(self, num_workers=1, speed_adj=100):
		self.start_time = time()
		self.sample_size = (self.original_load // speed_adj) if speed_adj else self.original_load

		self.add_workers(num_workers)
		if speed_adj:
			i = 1
			curr_speed = None
			while len(self.tasks):
				self.adjust_speed(curr_speed)
				print(f'doing run # {i}...')
				curr_speed = self.measure_speed()
				i += 1

		while len(self.results) < self.original_load:
			self.results.append(self.out_queue.get())

		print(f'target runtime {self.tta - self.start_time}, actual: {time() - self.start_time}')

	@staticmethod
	def flush_queue(source: Queue, target: list):
		while True:
			try:
				target.append(source.get_nowait())
			except:
				break

	def partial_run(self, num_tasks):
		TimelyPool.flush_queue(self.out_queue, self.results)
		tasks_to_do = min(num_tasks, self.original_load - len(self.results))
		elapsed_time  = 0
		if tasks_to_do:
			start_time = time()
			for _ in range(tasks_to_do):
				self.results.append(self.out_queue.get())
			elapsed_time = time() - start_time
			speed = tasks_to_do / elapsed_time if elapsed_time else float("inf")
		return tasks_to_do, elapsed_time

	def target_speed(self):
		time_remains = max(self.tta - time(), 1)
		tasks_remain = self.original_load - self.out_queue.qsize()
		target_speed = tasks_remain / time_remains if tasks_remain else None
		print(f'target speed = {target_speed}')
		return target_speed
	
	def adjust_speed(self, curr_speed=None):
		speed = curr_speed or self.measure_speed()
		target_speed = self.target_speed()
		while len(self.active_workers) < MAX_WORKERS:
			if not target_speed:
				return
			if speed < target_speed:
				num_workers = len(self.active_workers)
				extra_workers = (ceil(target_speed * 1.1 / speed * num_workers) - num_workers) or 1
				extra_workers = min(extra_workers, MAX_WORKERS - num_workers)
				self.add_workers(extra_workers)
				new_speed = self.measure_speed()
				if new_speed < speed:
					self.add_workers(-extra_workers)
				else:
					speed = new_speed
					continue
			elif speed > 1.5 * target_speed and len(self.active_workers) > 1:
				self.add_workers(-1)
				speed = self.measure_speed()

				if speed > target_speed:
					continue
				else:
					self.add_workers(1)
			break

	def measure_speed(self):
		tasks_done, elapsed_time = self.partial_run(self.sample_size)
		speed = tasks_done / elapsed_time if elapsed_time else float('inf')
		print(f'measured speed = {speed}')
		return speed
