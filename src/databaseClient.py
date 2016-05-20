
import json
import urllib
import urllib.parse
import urllib.request
from datetime import *
from helpers import *

__all__ = ["databaseClient"]

class databaseClient :
    
    def createDB(self):
        sql = '''DROP TABLE IF EXISTS "FundHistory";

                CREATE TABLE "FundHistory" (
                "FundCode" character varying(10) NOT NULL 
                ,"Name" character varying(50) NOT NULL 
                ,"FundDate" date NOT NULL 
                ,"Price" numeric NOT NULL 
                ,"RealPrice" numeric NOT NULL 
                ,"IncrementRate" numeric NOT NULL 
                ,"IncrementValue" numeric NOT NULL 
                ,CONSTRAINT PK_FundHistory PRIMARY KEY (FundCode, FundDate)
                );
                CREATE INDEX IK_FundHistory_FundDate_FundCode ON "FundHistory" (FundDate, FundCode);
                '''
               # ,"FundType" character varying(50) NOT NULL 
               # ,"LastDayPrice" numeric NOT NULL 
               # ,"FundStatus" character varying(50) NOT NULL 
        conn = sqliteHelper.connect()
        try:
            sqliteHelper.executescript(conn, sql)
        except Exception as e:
            print(e)
        finally:
            conn.close()
        
        print("Fund database is created.")
        
    def insertFundHistoryData(self, params):
        sql = '''
              REPLACE INTO FundHistory(
                  FundCode
                 ,Name
                 ,FundDate
                 ,Price
                 ,RealPrice
                 ,IncrementRate
                 ,IncrementValue
              ) VALUES (
                  ?,?,?,?,?,?,?
              )
              '''
   
        conn = sqliteHelper.connect()
        try:
            sqliteHelper.executemany(conn, sql, params)
        except Exception as e:
            print(e)
            print(sql)
        finally:
            conn.close()
        
        print("Fund history data are created.")
    
    def insert(self, fileName = "all_fund.txt"):
        funds = fundDataHelper.getFundHistory(fileHelper.read(spiderHelper._getFilePath(fileName)))
        newFunds = []
        for fund in funds :
            newFund=[fund[0], fund[1], date.today(), fund[5], fund[6], fund[8], fund[7]] 
            newFunds.append(newFund)
        self.insertFundHistoryData(newFunds)
   
    def run(self, sql = "SELECT COUNT(*) FROM FundHistory"):
        conn = sqliteHelper.connect()
        try:
            for row in sqliteHelper.execute(conn, sql):
                print(row)
        except Exception as e:
            print(e)
            print(sql)
        finally:
            conn.close()   
 
    def connect(self):
        import sqlite3
        try:
            dbPath = "C:/Tools/mm/jj.db"
            conn = sqlite3.connect(dbPath)
            for row in sqliteHelper.execute(conn, "SELECT * from SQLite_master"):
                print(row)
            #return conn
        except Exception as e:
            print(e)
            #raise
        #finally:
            
            #conn.close()    http://fund.eastmoney.com/f10/F10DataApi.aspx?type=gz&code=000001&rt=1463720719740  
        print("done")    
     
    
if __name__ == '__main__':
    import sys
    systemHelper.init()
    client = databaseClient()
    client.connect()
    #client.insert()
    #client.run()
