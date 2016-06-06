
import json
import urllib
import urllib.parse
import urllib.request
import logging
from datetime import *
from helpers import *
from fundInfoSpiderClient import *


__all__ = ["CreateDatabaseTask"]

class DbTask(Task):
    def __init__(self):
        super().__init__()
        
    def updateData(self, sql, params):
        conn = sqliteHelper.connect()
        try:
            sqliteHelper.executemany(conn, sql, params)
        except Exception as e:
            logging.getLogger().exception(e)
            logging.getLogger().error('Error! SQL = {}'.format(sql))
        finally:
            conn.close()

class CreateDatabaseTask(Task) :
    def __init__(self, reCreate=False):
        super().__init__()
        self.reCreate = reCreate
    
    def run(self):
        if (self.reCreate == False and fileHelper.exists(productContext.DatabaseFilePath)) :
            logging.getLogger().info('Database file "%s" already exists.', productContext.DatabaseFilePath)
            return
            
        sql = '''
        DROP TABLE IF EXISTS "FundHistory";

        DROP TABLE IF EXISTS "Company";

        DROP TABLE IF EXISTS "Manager";

        DROP TABLE IF EXISTS "ManagerFund";

        DROP TABLE IF EXISTS "Fund";

        CREATE TABLE "FundHistory" (
        "FundCode" character varying(10) NOT NULL 
        ,"FundName" character varying(50) NOT NULL 
        ,"FundDate" date NOT NULL 
        ,"FundType" character varying(50) NOT NULL   CONSTRAINT DF_FundHistory_FundType DEFAULT ('')
        ,"Price" numeric NOT NULL 
        ,"RealPrice" numeric NOT NULL 
        ,"IncrementRate" numeric NOT NULL 
        ,"IncrementValue" numeric NULL 
        ,CONSTRAINT PK_FundHistory PRIMARY KEY (FundCode, FundDate)
        ,CONSTRAINT FK_FundHistory_FundCode FOREIGN KEY (FundCode) REFERENCES Fund(FundCode)
        );
        CREATE INDEX IK_FundHistory_FundDate_FundCode ON "FundHistory" (FundDate, FundCode);

        CREATE TABLE "Company" (
        "CompanyCode" character varying(10) NOT NULL 
        ,"CompanyName" character varying(50) NOT NULL 
        ,"Pinyin" character varying(50) NOT NULL 
        ,"CreateDate" date NOT NULL 
        ,"Manager" character varying(50) NOT NULL 
        ,"Capital" numeric NULL 
        ,"FundCount" integer NOT NULL 
        ,CONSTRAINT PK_Company PRIMARY KEY (CompanyCode)
        );

        CREATE TABLE "Manager" (
        "ManagerCode" character varying(10) NOT NULL 
        ,"ManagerName" character varying(50) NOT NULL 
        ,"CompanyCode" character varying(50) NOT NULL 
        ,"CompanyName" character varying(50) NOT NULL 
        ,"WorkDate" date NOT NULL 
        ,"Description" character varying(1000) NOT NULL 
        ,CONSTRAINT PK_Manager PRIMARY KEY (ManagerCode)
        ,CONSTRAINT FK_Manager_CompanyCode FOREIGN KEY (CompanyCode) REFERENCES Company(CompanyCode)
        );

        CREATE TABLE "ManagerFund" (
        "ManagerCode" character varying(10) NOT NULL 
        ,"ManagerName" character varying(50) NOT NULL 
        ,"FundCode" character varying(50) NOT NULL 
        ,"FundName" character varying(50) NOT NULL 
        ,"FundType" character varying(50) NOT NULL 
        ,"StartDate" date NOT NULL 
        ,"EndDate" date NULL 
        ,"IncrementRate" numeric NOT NULL 
        ,"Days" integer NOT NULL 
        ,"FundCapital" numeric NOT NULL 
        ,"ThreeMonthRate" numeric NULL 
        ,"ThreeMonthRank" integer NULL 
        ,"ThreeMonthTotal" integer NULL 
        ,"SixMonthRate" numeric NULL 
        ,"SixMonthRank" integer NULL 
        ,"SixMonthTotal" integer NULL 
        ,"OneYearRate" numeric NULL 
        ,"OneYearRank" integer NULL 
        ,"OneYearTotal" integer NULL 
        ,"TwoYearRate" numeric NULL 
        ,"TwoYearRank" integer NULL 
        ,"TwoYearTotal" integer NULL 
        ,"ThisYearRate" numeric NULL 
        ,"ThisYearRank" integer NULL 
        ,"ThisYearTotal" integer NULL 
        ,CONSTRAINT PK_ManagerFund PRIMARY KEY (ManagerCode, StartDate, FundCode)
        ,CONSTRAINT FK_ManagerFund_FundCode FOREIGN KEY (FundCode) REFERENCES Fund(FundCode)
        );

        CREATE TABLE "Fund" (
        "FundCode" character varying(10) NOT NULL 
        ,"FundName" character varying(50) NOT NULL 
        ,"Pinyin" character varying(50) NOT NULL 
        ,"FundType" character varying(50) NOT NULL 
        ,"CompanyCode" character varying(10) NOT NULL 
        ,"CompanyName" character varying(50) NOT NULL 
        ,"ManagerName" character varying(50) NOT NULL 
        ,"IssueDate" date NOT NULL 
        ,"InitScale" numeric NOT NULL 
        ,"CurrentScale" numeric NOT NULL 
        ,"Capital" numeric NOT NULL 
        ,CONSTRAINT PK_Fund PRIMARY KEY (FundCode)
        ,CONSTRAINT FK_Fund_CompanyCode FOREIGN KEY (CompanyCode) REFERENCES Company(CompanyCode)
        );

        '''
        conn = sqliteHelper.connect()
        try:
            sqliteHelper.executescript(conn, sql)
        except Exception as e:
            logging.getLogger().exception(e)
            logging.getLogger().error('Error! SQL = {}'.format(sql))
        finally:
            conn.close()
        
        print("Fund database is created.")

class CompanyDataTask(DbTask):
    def __init__(self):
        super().__init__()
   
    def run(self):
        companiesJson = fileHelper.readjson(fundSpiderContext.getAllCompaniesFileName())
        companies = []
        for data in companiesJson :
            company=[data['code'], data['name'], data['pinyin'], data['createDate'], data['manager'], data['capital'], data['fundCount']] 
            companies.append(company)
        sql = '''
              REPLACE INTO Company(
                  CompanyCode
                , CompanyName
                , Pinyin
                , CreateDate
                , Manager
                , Capital
                , FundCount
              ) VALUES (
                  ?,?,?,?,?,?,?
              )
              '''
        self.updateData(sql, companies)
        
class ManagerDataTask(DbTask):
    def __init__(self):
        super().__init__()
   
    def run(self):
        managersJson = fileHelper.readjson(fundSpiderContext.getAllManagerFileName())
        managersInfoJson = fileHelper.readjson(fundSpiderContext.getManagerBasicInfoFileName())
        managers = []
        for data in managersJson.values() :
            managerInfo = managersInfoJson[data['code']]
            manager = [data['code'], data['name'], data['companyCode'], data['companyName'], managerInfo['managerStartDate'], managerInfo['description']] 
            managers.append(manager)
        sql = '''
              REPLACE INTO Manager(
                  ManagerCode
                , ManagerName
                , CompanyCode
                , CompanyName
                , WorkDate
                , Description
              ) VALUES (
                  ?,?,?,?,?,?
              )
              '''
        self.updateData(sql, managers)
        
class FundDataTask(DbTask):
    def __init__(self):
        super().__init__()
   
    def run(self):
        fundsJson = fileHelper.readjson(fundSpiderContext.getFundBasicInfoFileName())
        funds = []
        for data in fundsJson.values() :
            fund = [data['code'], data['name'], '', data['fundType'], 
                    data['companyCode'], data['company'], data['manager'], data['issueDate'],
                    data['initScale'], data['currentScale'], data['capital']] 
            funds.append(fund)      
        sql = '''
              REPLACE INTO Fund(
                  FundCode
                , FundName
                , Pinyin
                , FundType
                , CompanyCode
                , CompanyName
                , ManagerName
                , IssueDate
                , InitScale
                , CurrentScale
                , Capital
              ) VALUES (
                  ?,?,?,?,?,?,?,?,?,?,?
              )
              '''
        self.updateData(sql, funds)
        
class FundHistoryDataTask(DbTask):
    def __init__(self):
        super().__init__()
   
    def run(self):
        fundsJson = fileHelper.readjson(fundSpiderContext.getFundBasicInfoFileName())
        funds = []
        total = len(fundsJson.keys())
        i = 0
        for key in fundsJson.keys() :
            i +=1
            filename = fundSpiderContext.getFundHistoryFileName(key)
            if (not fileHelper.exists(filename)):
                logging.getLogger().warn("Fund '{}' does not have histroy data.".format(key))
                continue
            fundJson = fileHelper.readjson(filename)['history']
            for data in fundJson:
                if (data['realPrice'] != None):
                    fund = [data['code'], data['name'], data['date'], 
                            '', data['price'], data['realPrice'], data['incrementRate'], None] 
                    funds.append(fund)
            logging.getLogger().info("Processing {} / {}...".format(i, total))

        sql = '''
            REPLACE INTO FundHistory(
                FundCode
                , FundName
                , FundDate
                , FundType
                , Price
                , RealPrice
                , IncrementRate
                , IncrementValue
            ) VALUES (
                ?,?,?,?,?,?,?,?
            )
            '''
        self.updateData(sql, funds)
            
class ManagerFundDataTask(DbTask):
    def __init__(self):
        super().__init__()
   
    def run(self):
        managersJson = fileHelper.readjson(fundSpiderContext.getManagerBasicInfoFileName())
        funds = []
        fundPerformances = []
        for manager in managersJson.values() :
            managerCode = manager['code']
            managerName = manager['name']
            fundJson = manager['fundList']
            startDateDic = {}
            for data in fundJson:
                startDateDic[data['fundCode']] = data['startDate']
                fund = [managerCode, managerName, data['fundCode'], data['fundName'], data['fundType'], 
                        data['startDate'], data['endDate'], data['increment'], data['days'], data['fundMoney']] 
                funds.append(fund)

            fundJson = manager['fundPerformanceList']
            for data in fundJson:
                fund = [managerCode, data['fundCode'], startDateDic[data['fundCode']], 
                        data['threeMonthRate'], data['threeMonthRank'], data['threeMonthTotal'],
                        data['sixMonthRate'], data['sixMonthRank'], data['sixMonthTotal'],
                        data['oneYearRate'], data['oneYearRank'], data['oneYearTotal'],
                        data['twoYearRate'], data['twoYearRank'], data['twoYearTotal'],
                        data['thisYearRate'], data['thisYearRank'], data['thisYearTotal'] ] 
                fundPerformances.append(fund)               

        sql = '''
            REPLACE INTO ManagerFund(
                ManagerCode
                ,  ManagerName
                ,  FundCode
                ,  FundName
                ,  FundType
                ,  StartDate
                ,  EndDate
                ,  IncrementRate
                ,  Days
                ,  FundCapital
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?
            )
            '''
        self.updateData(sql, funds)

        sql = '''
            REPLACE INTO ManagerFund(
                ManagerCode
                ,  FundCode
                ,  StartDate
                ,  ThreeMonthRate
                ,  ThreeMonthRank
                ,  ThreeMonthTotal
                ,  SixMonthRate
                ,  SixMonthRank
                ,  SixMonthTotal
                ,  OneYearRate
                ,  OneYearRank
                ,  OneYearTotal
                ,  TwoYearRate
                ,  TwoYearRank
                ,  TwoYearTotal
                ,  ThisYearRate
                ,  ThisYearRank
                ,  ThisYearTotal
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            '''
        self.updateData(sql, fundPerformances)

class ValidateTask(Task):
    def run(self):
        tables = [
                "Company",
                "Manager",
                "Fund",
                'FundHistory',
                'ManagerFund'
                ]
        conn = sqliteHelper.connect()
        try:
            for table in tables:
                sql = "SELECT COUNT(*) FROM {}".format(table)
                for row in sqliteHelper.execute(conn, sql):
                    logging.getLogger().info('Total {}: {}'.format(table, row))
        except Exception as e:
            logging.getLogger().exception(e)
            logging.getLogger().error('Error! SQL = {}'.format(sql))
        finally:
            conn.close()   
    
if __name__ == '__main__':
    import sys
    systemHelper.init()
    with TaskManager(
        [
        # CreateDatabaseTask(reCreate=True), 
        # CompanyDataTask(),
        # ManagerDataTask(),
        # FundDataTask(),
        FundHistoryDataTask(),
        ManagerFundDataTask(),
        ValidateTask()
        ]) as task:
        task.run()
    #client.insert()
    #client.run()
    systemHelper.end()
