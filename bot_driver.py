#!/usr/bin/python3 -u
'''
A simple demo bot driver
'''
import time
from bot import Pool
import sys


command = ['./echo_bot.py']
tasks = list(range(50))
num_workers = 1
setup = 'hello'
pool = Pool(command, tasks, num_workers, setup=setup, logstream=sys.stdout)

while pool.tasks.unfinished_tasks:
	print(f'progress: {pool.results.qsize()}')
	time.sleep(5)

results = [None] * len(tasks)
while not pool.results.empty():
	i, value = pool.results.get()
	results[i] = value

for x, y in zip(tasks, results):
	print(x, y)



