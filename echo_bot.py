#!/usr/bin/python3 -u
'''
A simple worker which 
'''
from bot import start_bot
from sys import stderr
import time


def preprocessor(data):
	return data.upper() 


def processor(query, param):
	return time.strftime(f'%Y%m%d%H%M%S: {param} {query}!!!')


if __name__ == '__main__':
	start_bot(processor, preprocessor, logstream=stderr)