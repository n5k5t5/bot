#!/usr/bin/python3 -u
'''
A simple demo bot driver
'''
import time
from bot import TimelyPool
import sys, os


command = [f'{os.path.dirname(__file__)}/echo_bot.py']
tasks = list(range(1000000))
num_workers = 1

setup = 'hello'
target_runtime = 100
tta = time.time() + target_runtime # target time of arrival
pool = TimelyPool(command, tasks, setup=setup, tta=tta)
pool.run(num_workers=num_workers, speed_adj=10)
