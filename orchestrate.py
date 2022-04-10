# some_file.py
import sys
import random
import multiprocessing
import main
import multiprocessing

# batch1='EVERY_MINUTE1'
batch2='EVERY_MINUTE2'

NUM_PROC = [batch2]

def runjob(func,arg):
	jobs = []
	for batch in arg:
		process = multiprocessing.Process(
			target=func, 
			args=(batch,)
		)
	jobs.append(process)

	for j in jobs:
		j.start()

	for j in jobs:
		j.join()


if __name__ == "__main__":
	while True:
		runjob(main.mjEdlIngest,NUM_PROC)

