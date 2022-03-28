# some_file.py
import sys
import random
import multiprocessing
import main
import multiprocessing

batch1='EVERY_MINUTE1'
# main.mjEdlIngest(batch1)

batch2='EVERY_MINUTE2'
# main.mjEdlIngest(batch2)


NUM_PROC = [batch1,batch2]


# def append_to_list(lst, num_items):
#     print("A")
#     for n in random.sample(range(20000000), num_items):
#         lst.append(n)

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


# for batch in batch1,batch2:
# 	print(batch)