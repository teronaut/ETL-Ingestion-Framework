from time import sleep
import ingestion
import snowflake.connector
import os
import json
from ddlgenerator.ddlgenerator import Table
import re
import multiprocessing

environmentVariables='config/envVar.json'
ev={"controlTable":"","cur":""}
def executeMultiQuery(queries):
    cur=ev["cur"]
    dbObjCur=cur
    for i in queries:
        dbObjCur.execute(i)
        print(dbObjCur.fetchall())
    return dbObjCur.fetchone()

def get_ddl(fileSourceLocation,fileName,targetDb,targetSchema,targetTable):
    menu = Table(str(fileSourceLocation)+"/"+str(fileName),table_name=targetTable)
    ddl = menu.ddl('mysql')
    ddl = ddl.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS "+str(targetDb)+"."+str(targetSchema)+".")
    ddl = ddl.replace(";","")
    # ddl = ddl.replace("DROP TABLE  "+str(fileName),"")
    ddl=re.sub('.*', '', ddl, 3)
    # print(re.sub('^DROP TABLE .*$', '', ddl))
    return(ddl)


def FullDirectFile(jobVariables):
    fileSourceType = jobVariables['FILE_SRC_TYPE']
    fileSourceLocation = jobVariables['SRC_LOC']
    fileName = jobVariables['SRC_TBL_NAME']
    stageName = jobVariables['STAGE']
    targetTable = str(jobVariables['TGT_TBL'])
    targetSchema = jobVariables['TGT_SCHEMA']
    targetDb = jobVariables['TGT_DB']
    fileFormat = jobVariables['FILE_FORMAT']
    fileOptions = jobVariables['FILE_OPTIONS']
    truncateFlag= jobVariables['TARGET_TRUNCATE_FLAG']
    recreateDdl=jobVariables['RECREATE_DDL']
    fileFormatDb=jobVariables['FILE_FORMAT_DB']
    fileFormatSchema=jobVariables['FILE_FORMAT_SCHEMA']
    # menu = Table(str(fileSourceLocation)+"/"+str(fileName),table_name=targetTable)
    # ddl = menu.ddl('mysql')
    # ddl = ddl.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS "+str(targetDb)+"."+str(targetSchema)+".")
    # ddl = ddl.replace(";","")
    # # ddl = ddl.replace("DROP TABLE  "+str(fileName),"")
    # ddl=re.sub('.*', '', ddl, 3)
    # # print(re.sub('^DROP TABLE .*$', '', ddl))
    queries=[]
    if(recreateDdl=='Y'):
        ddl= get_ddl(fileSourceLocation,fileName,targetDb,targetSchema,targetTable)
        queries.append(ddl)
    elif(truncateFlag=='Y'):
        truncateQuery="TRUNCATE TABLE "+targetDb+"."+targetSchema+"."+targetTable
        queries.append(truncateQuery)
    if(fileSourceType=='Local'):
        putCommand="PUT 'file://"+str(fileSourceLocation)+"/"+str(fileName)+"' @"+stageName
        copyCommand="COPY INTO "+targetDb+"."+targetSchema+"."+targetTable+" FROM '@"+stageName+"/"+fileName+".gz' FILE_FORMAT="+fileFormatDb+"."+fileFormatSchema+"."+fileFormat+" "+fileOptions
        queries.append(putCommand)
        queries.append(copyCommand)
    print(queries)    
    executeMultiQuery(queries)

def readControlTable(CONTROL_TABLE,batch):
    import csv
    file = open(CONTROL_TABLE)
    csvreader = csv.DictReader(file)
    # print(header)
    rows = []
    for row in csvreader:
        # print(row['BATCH_NAME'])
        if((row['BATCH_NAME']==batch) & (row['JOB_ACTIVE_FLAG'] =='Y')):
            rows.append(row)
    # print(rows)
    file.close()
    return rows
   
def gridIterator(gridVariable,func):
    # num_cores = multiprocessing.cpu_count()
    # results = Parallel(n_jobs=num_cores)(delayed(func)(jobVariable) for jobVariable in gridVariable)
    # jobs=[]
    # for jobVariable in gridVariable:
    #     process = multiprocessing.Process(target=func,args=(jobVariable))
    #     jobs.append(process)
    #     for j in jobs:
    #         j.start()
    #     for j in jobs:
    #         j.join()
    jobs = []
    for batch in gridVariable:
        # print(batch)
        process = multiprocessing.Process(
			target=func, 
			args=(batch,)
		)
        jobs.append(process)
    for j in jobs:
        j.start()
    for j in jobs:
        j.join()
    # for jobVariable in gridVariable:
    #     func(jobVariable)
        # try:
        #     func(jobVariable,cur)
        #     continue
        # except:
        #     print("an error occured")
        #     continue

    return 0

def initEnv(env,environmentVariables):
    f = open(environmentVariables)
    envVar = json.load(f)
    ev["controlTable"]=str(envVar[env]["CONTROL_TABLE"])
    wh="USE WAREHOUSE "+str(envVar[env]["DEFAULT_WH"])
    dbSch="USE "+str(envVar[env]["DEFAULT_DB"])+"."+str(envVar[env]["DEFAULT_SCHEMA"])
    queries=['SELECT current_version()',wh,dbSch]
    sfSrc=ingestion.dbConnect(str(envVar[env]["CONFIG_FILE"]),str(envVar[env]["TGT_CON_NAME"]),logging=True,debug=False)
    cur=sfSrc.dbCursor
    ev["cur"]=cur
    executeMultiQuery(queries)
    return cur


def mjEdlIngest(batch):
    gridVariable=readControlTable(ev["controlTable"],batch)
    # for i in gridVariable:
        # print(i["JOB_NAME"])
    gridIterator(gridVariable,FullDirectFile)



import multiprocessing
batch='EVERY_MINUTE1'
# global cur
initEnv('DEV_AZURE_WEST',environmentVariables)
if __name__ == "__main__":
    # initEnv('DEV_AZURE_WEST',environmentVariables)
    mjEdlIngest(batch)

	
# while True:
#     mjEdlIngest(batch,cur)
#     sleep(50)
# mjEdlIngest(batch)



# gridIterator(gridVariable,FullDirectFile,sfSrc.dbCursor)
# FullDirectFile(fileSourceType,fileSourceLocation,fileName,stageName,targetTable,targetSchema,targetDb,fileFormat,fileOptions,sfSrc.dbCursor,'Y')
# sfSrc.dbCursor.execute("PUT 'file://D:/Workspace/Software Development/akash-adhikary/Projects/ETL/ETL-Ingestion-Framework/test_file.csv' @OYO.USERS.CONTACTS")
# sfSrc.dbCursor.execute("copy into employee from '@contacts/test_file.csv.gz' file_format=DEMO_CONTACTS_CSV PURGE=TRUE")
# print(sfSrc.dbCursor.fetchall())


