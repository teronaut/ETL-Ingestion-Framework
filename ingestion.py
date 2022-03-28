#!/usr/bin/env python
from sre_constants import SUCCESS
import snowflake.connector
import json
from datetime import datetime

class dbConnect:
    
    def __init__(self,path2Config,conName,logging=False,debug=False):
        self.conName = conName
        self.logging=logging
        self.debug=debug
        self.configFile=self.readConfig(path2Config)
        self.__conDetails=self.getConDtls(conName)
        self.dbCursor=self.con2Db()

    def log(self,op):
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        op = str(current_time) +" : "+str(op)
        if (self.debug==True):
            print(op)
        if (self.logging==True):
            f = open("log\\etl_ingestion_fw.log", "a")
            f.write(f'\n{op}')
            f.close()


    def readConfig(self,path2Config):
        try:
            f = open(path2Config)
            data = json.load(f)
            self.log("successfully opened config file")
            f.close()
            return(data)
        except:
            self.log("something went wrong could not open the file '"+path2Config+"'")
            return(1)
            

    def getConDtls(self,conName):
        try:
            for db in self.configFile["dbDetails"]:
                if (db["dbName"]==conName):
                    conDetails=db
                    self.log(conDetails)
                    return(conDetails)
            self.log("db '"+conName+"' not recognised, please check the config file")
            return(1)
        except:
            self.log("error parsing the config file")
            
        
    def con2Db(self):
        if self.__conDetails["dbType"]=="snowflake":
            self.log("dbType = "+ str(self.__conDetails["dbType"]))
            ctx = snowflake.connector.connect(
            user=self.__conDetails["auth"]["user"],
            password=self.__conDetails["auth"]["password"],
            account=self.__conDetails["auth"]["account"]
            )
            cs = ctx.cursor()
            try:
                cs.execute("SELECT current_version()")
                one_row = cs.fetchone()
                self.log("snowflake version = "+str(one_row[0]))
                return (cs)
            except:
                cs.close()
            ctx.close()

        elif self.__conDetails["dbType"]=="snowflake":
            self.log("dbType = "+ str(self.__conDetails["dbType"]))
        else: 
            self.log("dbType not Recognized")

    # def conDynamincDb(self, conDtls):
    #     if(conType=='snowflake'):
    #         ctx = snowflake.connector.connect(
    #         user='akashadhikary',
    #         password='Teromeet17217@#',
    #         account='lm04575.ap-south-1.aws'
    #         )
    #         cs = ctx.cursor()
    #         try:
    #             cs.execute("SELECT current_version()")
    #             one_row = cs.fetchone()
    #             print(one_row[0])
    #         finally:
    #             cs.close()
    #         ctx.close()
            

    def conStatus(self):
        self.log("connected")


