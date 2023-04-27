'''
Python package for running workers
'''
from sys import stdin, stdout
import os
import pickle
from queue import Queue
from subprocess import Popen, PIPE
from threading import Thread
from time import sleep


HEADERSIZE = 64
BYTE_ORDER = 'little'
PREP = 'prep'
QUERY = 'query'


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
	write_log(f'wrote msg of len {len(raw_msg)}')
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
		msg, success = read_msg(stdin.buffer, logstream=logstream)
		if not success:
			write_log('eof')
			return
		kind, data = msg
		if kind == PREP and preprocessor:
			params = preprocessor(data)
			send_msg([kind, True], stdout.buffer, logstream=logstream)
		elif kind == QUERY:
			response = processor(data, params) if preprocessor else processor(data) 
			send_msg([kind, response], stdout.buffer, logstream=logstream)


def man_bot(process, param, in_queue, results, logstream=None):
	print(f'manning process {process.pid}')
	if param:
		send_msg([PREP, param], process.stdin, logstream=logstream)
		msg, success = read_msg(process.stdout, logstream=logstream)
		assert success
	while in_queue.unfinished_tasks:
		i, job = in_queue.get()
		print(f'got job {i} from the queue')
		try:
			send_msg([QUERY, job], process.stdin, logstream=logstream)
			print('sent...')
			msg, success = read_msg(process.stdout, logstream=logstream)
			assert success
			kind, result = msg
		except Exception as ex:
			in_queue.put([i, job])
			print(ex)
			sleep(5)
		else:
			results.put([i, result])
			in_queue.task_done()
			print(f'task {i} is done')
	process.stdin.close()
		

class Pool:
	def __init__(self, command, tasks, num_workers, setup=None, logstream=None):
		self.tasks = Queue()
		for i, task in enumerate(tasks):
			self.tasks.put([i, task])
		self.results = Queue()
		self.processes = []
		self.threads = []
		for i in range(num_workers):
			process = Popen(command, stdin=PIPE, stdout=PIPE, text=False)
			print(f'launched process with pid = {process.pid}')
			thread = Thread(target=man_bot, args=[process, setup, self.tasks, self.results], kwargs={'logstream': logstream}, daemon=True)
			thread.start()
			self.processes.append(process)
			self.threads.append(thread)
