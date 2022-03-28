from ddlgenerator.ddlgenerator import Table


# import csv
# file = open('control/ingest.csv')
# csvreader = csv.DictReader(file)
# rows = []
# for row in csvreader:
#         rows.append(row)
# table=row
# print(row)
# file.close()


# menu = Table('../bucket/common/groups.csv',table_name='GRP')
# ddl = menu.ddl('postgres')
# inserts = menu.inserts('mysql')
# all_sql = menu.sql('mysql', inserts=True)
# print(ddl)

# table = Table([{"Name": "Alfred", "species": "wart hog", "kg": 22}])
# sql = table.sql('postgresql', inserts=True)
# print(sql)


# import re

# str = 'aaa@gmail.com'

# print(re.sub('.*', '', ddl, 3))

# from joblib import Parallel, delayed
# import multiprocessing
# # what are your inputs, and what operation do you want to
# # perform on each input. For example...
# def processInput(i):
#     return i * i


# def proc(func):
#     inputs = range(10)
#     num_cores = multiprocessing.cpu_count()
#     results = Parallel(n_jobs=num_cores)(delayed(func)(i) for i in inputs)
#     print(results)

# proc(processInput)



import random
import multiprocessing


NUM_PROC = 2


def append_to_list(lst, num_items):
    for n in random.sample(range(20000000), num_items):
        lst.append(n)


if __name__ == "__main__":
	jobs = []

	for i in range(NUM_PROC):
		process = multiprocessing.Process(
			target=append_to_list, 
		    args=([], 10000000)
		)
		jobs.append(process)

	for j in jobs:
		j.start()

	for j in jobs:
		j.join()

