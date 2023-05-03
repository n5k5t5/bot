#!/usr/bin/python3 -u
'''
A simple demo bot driver
'''
import time
from bot import TimelyPool
import sys, os


command = [f'{os.path.dirname(__file__)}/echo_bot.py']
tasks = list(range(100000))

setup = 'hello'
target_runtime = 100
tta = time.time() + target_runtime # target time of arrival
pool = TimelyPool(command, tasks, setup=setup).start(num_workers=4, tta=tta)

while not pool.is_done():
    time.sleep(5)
    print(f'progress: {pool.results.qsize()}', flush=True)

results = pool.get_results()
print(results[:10])
