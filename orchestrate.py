# some_file.py
import sys
import random
import multiprocessing
import main
import multiprocessing
import batches

NUM_PROC =batches.NUM_PROC

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
		runjob(main.mjEdlIngest,NUM_PROC)

