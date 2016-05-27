import json
import urllib
import urllib.parse
import urllib.request
import time
import math
import logging
import threading
import inspect
import os
from concurrent.futures import *
from datetime import date, datetime, timedelta
from helpers import *
from bs4 import BeautifulSoup
from threading import Lock

__all__ = ["fundSpiderContext"]


class fundSpiderContext:
    configuration_file = "fund_spider.json"
    host = "http://fund.eastmoney.com"
    all_funds_html_link = "{0}/fund.html#os_0;isall_1;ft_;pt_1"
    property_fail_funds = "fail_funds"

    def __init__(self):
        self.confile = os.path.dirname(inspect.getfile(
            inspect.currentframe())) + "/" + fundSpiderContext.configuration_file
        con = fileHelper.read(self.confile)
        self.configuration = json.loads(con)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        con = json.dumps(self.configuration, ensure_ascii=False, indent=4)
        fileHelper.save(self.confile, con)

    def getAllCompaniesLink() :
        return "{}/Data/FundRankScale.aspx".format(fundSpiderContext.host)
        
    def getAllCompaniesFileName() :
        return spiderHelper.getFilePath("all_companies.json")

    def getCompanyToFundLink(companyCode) :
        return "{}/company/{}.html".format(fundSpiderContext.host, companyCode)
        
    def getCompanyToFundFileName(companyCode) :
        return spiderHelper.getFilePath("/company/company_fund_{}.json".format(companyCode))
        
    def getAllFundsDataLink(day=date.today()):
        return "{}/Data/Fund_JJJZ_Data.aspx??t=1&lx=1&letter=&gsid=&text=&sort=zdf,desc&page=1,9999&feature=|&dt={}&atfc=&onlySale=0".format(
            fundSpiderContext.host,
            int(time.mktime(day.timetuple())))

    def getAllFundsFileName(day=date.today()):
        return spiderHelper.getFilePath("all_funds.json".format(typeHelper.toStr(day, '%Y_%m_%d')))

    def getFundHistoryDataLink(fundCode, page, count, startDate, endDate):
        return "{}/f10/F10DataApi.aspx?type=lsjz&code={}&page={}&per={}&sdate={}&edate={}&rt=0.17680868120777338".format(
            fundSpiderContext.host,
            fundCode, page, count, typeHelper.toStr(startDate), typeHelper.toStr(endDate))

    def getFundHistoryFileName(fundCode):
        return spiderHelper.getFilePath("/history/history_{}.json".format(fundCode))

    def getFundBasicInfoLink(fundCode):
        return "{}/f10/jbgk_{}.html".format(fundSpiderContext.host, fundCode)

    def getFundBasicInfoFileName(fundCode = None):
        if (fundCode == None) :
            return spiderHelper.getFilePath("/funds_info.json")
        return spiderHelper.getFilePath("/fund/fund_{}.json".format(fundCode))

    def getProperty(self, key):
        return self.configuration[key]

    def setProperty(self, key, value):
        self.configuration[key] = value

    def test(self, filename='fund_000001_001.txt'):
        content = fileHelper.read(spiderHelper.getFilePath(filename))
        content = content[content.find("{"): content.rfind("}") + 1]
        content = content.replace('content:', '"content":')
        content = content.replace('records:', '"records":')
        content = content.replace('pages:', '"pages":')
        content = content.replace('curpage:', '"curpage":')
        self.storeFundHistory('', "", content)

    def storeFundHistory(self, fundCode, fundName, jsonStr):
        jsonObject = json.loads(jsonStr)
        content = jsonObject["content"]
        soup = BeautifulSoup(content, "html.parser")
        fundHistory = []
        for tr in soup.tbody.find_all('tr'):
            data = []
            for td in tr.find_all('td'):
                data.append(td.text)
            fund = [fundCode,
                    fundName,
                    typeHelper.toDate(data[0]),
                    float(data[1]),
                    float(data[2]),
                    float(data[3][0:data[3].rfind('%')]),
                    None
                    ]
            print(fund)
            fundHistory.append(fund)


class FundThreadCallbackHandler:

    def __init__(self, client_id):
        self.client_id = client_id

    def handle_callback(self, future):
        pass
        # try:
        #     if (future.exception() != None):
        #         logging.getLogger().error("Failed {0}. Exception : {1}".format(
        #             self.client_id, future.exception()))
        # except CancelledError as e:
        #     logging.getLogger().error("Cancelled {0}".format(self.client_id))
        # logging.getLogger().info("Done {0}".format(self.client_id))


class task:

    def __init__(self, context):
        self.context = context

    def run(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class RequestAllFundsTask(task):

    def __init__(self, context):
        self.context = context

    def run(self):
        self.day = date.today()
        filename = fundSpiderContext.getAllFundsFileName()
        spiderHelper.saveRequest(
            fundSpiderContext.getAllFundsDataLink(self.day),
            "utf-8",
            filename,
            self.parseToJson)

    def parseToJson(self, content):
        fundsString = content[content.find("[["): content.find("]]") + 2]
        funds = eval(fundsString)
        fundList = {"date": typeHelper.toStr(self.day),
                    "funds": []}
        jsonFunds = fundList["funds"]
        for fund in funds:
            jsonFund = {"code": fund[0],
                        "name": fund[1],
                        "pinyin": fund[2]}
            jsonFunds.append(jsonFund)
        return json.dumps(fundList, ensure_ascii=False, indent=4)

class RequestFundsHistoryTask(task):

    def __init__(self, context, debug = False):
        self.lock = Lock()
        self.context = context
        self.debug = debug
        self.totalFunds = 0
        self.successFunds = 0
        self.startDate = date(2010, 1, 1)
        self.endDate = date.today() - timedelta(1)
        self.failedFunds = context.getProperty(fundSpiderContext.property_fail_funds)
        if (self.failedFunds == None):
            self.failedFunds = {}

    def run(self):
        funds = []
        if (len(self.failedFunds) == 0):
            allFundsJson = self.readAllFunds(
                fundSpiderContext.getAllFundsFileName())
            funds = allFundsJson["funds"]
        else :
            for key in self.failedFunds:
                fund = {"code": key, "name": self.failedFunds[key]}
                funds.append(fund)

        funds = sorted(funds, key=lambda item: item['code'])
        self.totalFunds = len(funds)
        self._requestFundsHistory(funds)
        self.saveConfiguration()
        
        if (self.debug == False):
            tryTimes = 0
            while (tryTimes < 3 and len(self.failedFunds) > 0):
                tryTimes += 1
                for key in self.failedFunds:
                    fund = {"code": key, "name": self.failedFunds[key]}
                    funds.append(fund)
                self._requestFundsHistory(funds)
                self.saveConfiguration()
        
        failedCount = len(self.failedFunds)
        logging.getLogger().info("Task RequestFundsHistoryTask finish. \n"
                            +  "    Start Date: %s\n"
                            +  "    End Date: %s\n"
                            +  "    Total: %d\n"
                            +  "    Success: %d\n"
                            +  "    Failure: %d",
                            typeHelper.toStr(self.startDate),
                            typeHelper.toStr(self.endDate),
                            self.totalFunds, self.totalFunds - failedCount, failedCount)

    def saveConfiguration(self):
        if (len(self.failedFunds) == 0):
            self.context.setProperty(
                fundSpiderContext.property_fail_funds, None)
        else:
            self.context.setProperty(
                fundSpiderContext.property_fail_funds, self.failedFunds)
        self.context.close()

    def _requestFundsHistory(self, funds):
        self.failedFunds = {}
        i = 0
        if (self.debug):
            for fund in funds:
                # try:
                if (i == 3):
                    break
                i += 1
                fundCode = fund["code"]
                fundName = fund["name"]
                self.failedFunds[fundCode] = fundName
                logging.getLogger().debug("Request fund %d : '%s'", i, fundCode)
                self.requestFundHistory(
                    fundCode,
                    fundName,
                    fundSpiderContext.getFundHistoryFileName(fundCode))
        else:
            with ThreadPoolExecutor(max_workers=10) as executor:
                for fund in funds:
                    try:
                        i += 1
                        fundCode = fund["code"]
                        fundName = fund["name"]
                        self.failedFunds[fundCode] = fundName
                        logging.getLogger().debug("Request fund %d : '%s'", i, fundCode)
                        future = executor.submit(
                            self.requestFundHistory,
                            fundCode,
                            fundName,
                            fundSpiderContext.getFundHistoryFileName(fundCode))
                        future.add_done_callback(
                            FundThreadCallbackHandler(fundCode).handle_callback)
                    except Exception as e:
                        logging.getLogger().warn(
                            "Failed {0}. {1}".format(fundCode, e))

            # wait all threads finish.
            executor.shutdown(wait=True)

    def readAllFunds(self, fileName):
        return json.loads(fileHelper.read(fileName))

    def requestFundHistory(self, fundCode, fundName, filename):
        content = ""
        try:
            fileHelper.delete(filename + ".txt")
            fundHistory = {}
            startDate = self.startDate
            endDate = self.endDate
            lastStartDate = None
            lastEndDate = None
            if (fileHelper.exists(filename)) :
                fundHistory = fileHelper.readjson(filename)
                lastStartDate = typeHelper.toDate(fundHistory['startDate'])
                lastEndDate = typeHelper.toDate(fundHistory['endDate'])
                startDate = lastEndDate + timedelta(1)
                if (startDate > endDate):
                    self.onSuccess(fundCode)
                    return
            else:
                fundHistory = {'history' : []}
            
            page = 1
            content = spiderHelper.saveRequest(
                fundSpiderContext.getFundHistoryDataLink(fundCode,
                                                        page,
                                                        100,
                                                        startDate,
                                                        endDate),
                "gb2312")

            jsonObject, history = self.getFundHistoryJson(content, fundCode, fundName)
            records = int(jsonObject["records"])
            pages = int(jsonObject["pages"]) + 1
            for page in range(2, pages):
                content = spiderHelper.saveRequest(
                    fundSpiderContext.getFundHistoryDataLink(fundCode,
                                                            page,
                                                            100,
                                                            self.startDate,
                                                            self.endDate),
                    "gb2312")
                jsonObject, newHistory = self.getFundHistoryJson(content, fundCode, fundName)
                history = history + newHistory

            if (len(history) > 0):
                lastEndDate = history[0]['date']
                fundHistory['endDate'] = lastEndDate
                if (lastStartDate == None):
                    lastStartDate = history[len(history) -1]['date']
                    fundHistory['startDate'] = lastStartDate
                fundHistory['history'] = history + fundHistory['history']
                fileHelper.savejson(filename, fundHistory)
            self.onSuccess(fundCode)
        except Exception as e:
            fileHelper.save(filename + ".txt", content)
            if (self.debug):
                raise
            logging.getLogger().exception("Failed {0}. Exception : {1}".format(fundCode, e))

    def onSuccess(self, fundCode):
        with self.lock :
            self.failedFunds.pop(fundCode)
            self.successFunds += 1
            logging.getLogger().info("Done ({}/{}) '{}'".format(self.successFunds, self.totalFunds,fundCode))
        
    def getFundHistoryJson(self, fundInfo, fundCode, fundName):
        fundInfo = fundInfo[fundInfo.find("{"): fundInfo.rfind("}") + 1]
        fundInfo = fundInfo.replace('content:', '"content":')
        fundInfo = fundInfo.replace('records:', '"records":')
        fundInfo = fundInfo.replace('pages:', '"pages":')
        fundInfo = fundInfo.replace('curpage:', '"curpage":')
        jsonObject = json.loads(fundInfo)
        content = jsonObject["content"]
        soup = BeautifulSoup(content, "html.parser")
        fundHistory = []
        for tr in soup.tbody.find_all('tr'):
            data = []
            tds = tr.find_all('td')
            if (len(tds) < 3):
                break
            for td in tds:
                data.append(td.text)
            
            if (len(data[1]) == 0):
                # The price does not provided for this day.
                continue
            realPrice = None # publish price
            if (len(data[2]) > 0):
                realPrice =  float(data[2])
            fund = {'code': fundCode,
                    'name': fundName,
                    'date': data[0],
                    'price': float(data[1]),
                    'realPrice': realPrice,
                    'incrementRate': float(data[3][0:data[3].rfind('%')]) if len(data[3]) > 0 else 0.0
                    }
            fundHistory.append(fund)

        return jsonObject, fundHistory

class RequestAllCompaniesTask(task):
    def __init__(self, context):
        self.context = context

    def run(self):
        self.day = date.today()
        spiderHelper.saveRequest(
                fundSpiderContext.getAllCompaniesLink(),
                "utf-8",
                fundSpiderContext.getAllCompaniesFileName(),
                self.parseToJson)
            
    def parseToJson(self, content):
        companyiesString = content[content.find("[["): content.find("]]") + 2]
        items = eval(companyiesString)
        companies = []
        for data in items:
            compnay = {'code': data[0],
                        'name' : data[1],
                        'pinyin' : data[5],
                        'createDate' : data[2],
                        'capital' : float(data[7]) if len(data[7]) > 0 else None,
                        'manager' : data[4],
                        'fundCount' : int(data[3]) if len(data[3]) > 0 else 0}
            companies.append(compnay)
            
        logging.getLogger().info("Found companies %d.", len(companies))
        return json.dumps(companies, ensure_ascii=False, indent=4, sort_keys=True)
        
class RequestCompanyToFundTask(task):
    def __init__(self, context, debug=False):
        self.context = context
        self.debug = debug
        self.code = ""
        self.name = ""
        self.fundCount = 0
        
    def run(self):
        self.day = date.today()
        companies = fileHelper.readjson(fundSpiderContext.getAllCompaniesFileName())
        i  = 0
        for company in companies:
            i += 1
            if (self.debug and i > 1):
                break;
            self.code = company['code']
            self.name = company['name']
            self.fundCount = company['fundCount']
            
            spiderHelper.saveRequest(
                fundSpiderContext.getCompanyToFundLink(self.code),
                "gb2312",
                fundSpiderContext.getCompanyToFundFileName(self.code),
                self.parseToJson)
            logging.getLogger().info("Done. %s.", self.code)
            
    def parseToJson(self, content):
        content = content.replace('</br>', '<br/>')
        soup = BeautifulSoup(content, "html.parser")
        companyFundList = {}
        for table in soup.find_all('table', class_='data_table'):
            data = []
            for td in table.find_all('td', class_='txt_left'):
                fundCode = td.text[len(td.a.text):len(td.text)]
                compnayFund = {'companyCode' : self.code,
                            'companyName' : self.name,
                            'fundCode': fundCode,
                            'fundName' : td.a.text}
                companyFundList[fundCode] = compnayFund
        if (self.fundCount == len(companyFundList)):
            logging.getLogger().info("'%s' fund count %d matched.", self.code, self.fundCount)
        else :
            logging.getLogger().error("'%s' fund count %d does not match %d.", self.code, self.fundCount, len(companyFundList))
            
        return json.dumps(companyFundList, ensure_ascii=False, indent=4, sort_keys=True)
 
class RequestFundBasicInfoTask(task):
    def __init__(self, context, debug=False):
        self.lock = Lock()
        self.context = context
        self.debug = debug
        self.failedFunds = {}
        self.fundList = {}
        self.successFunds = 0
        self.fundsCount = 0
        
    def run(self):
        self.day = date.today()
        funds = fileHelper.readjson(fundSpiderContext.getAllFundsFileName())['funds']
        self.fundsCount = len(funds)
        i  = 0
        with ThreadPoolExecutor(max_workers=10) as executor:
            for fund in funds:
                try:
                    i += 1
                    if (self.debug and i > 10):
                        break;
                    
                    fundCode = fund['code']
                    fundName = fund['name']
                    future = executor.submit(
                        self.requestFundInfo,
                        fundCode)
                except Exception as e:
                    logging.getLogger().warn(
                        "Failed {0}. {1}".format(fundCode, e))

        # wait all threads finish.
        executor.shutdown(wait=True)
            
        fileName = fundSpiderContext.getFundBasicInfoFileName()
        fileHelper.savejson(fileName, self.fundList)
        logging.getLogger().info("Total: {}".format(self.successFunds))
            
    def requestFundInfo(self, fundCode):
        filename = fundSpiderContext.getFundBasicInfoFileName(fundCode)
        content = ""
        try :
            fileHelper.delete(filename + ".txt")
            content = spiderHelper.saveRequest(
                        fundSpiderContext.getFundBasicInfoLink(fundCode),
                        "gb2312")
            fund = self.parseToJson(content, fundCode)
            self.fundList[fundCode] = fund
            self.onSuccess(fundCode)
        except Exception as e:
            fileHelper.save(filename + ".txt", content)
            logging.getLogger().exception("Failed {0}. Exception : {1}".format(fundCode, e))

    def onSuccess(self, fundCode):
        with self.lock :
            #self.failedFunds.pop(fundCode)
            self.successFunds += 1
            logging.getLogger().info("Done ({}/{}) '{}'".format(self.successFunds, self.fundsCount, fundCode))

    def parseToJson(self, content, fundCode):
        soup = BeautifulSoup(content, "html.parser")
        table = soup.find('table', class_='info w790')
        
        data = []
        for td in table.find_all('td'):
            data.append(td)
        fund = {
                'code' : fundCode,
                'name' : data[1].text,
                'fullname' : data[0].text,
                'issueDate' : data[4].text.replace('年', '-').replace('月', '-').replace('日', ''),
                'manager' : data[10].text,
                'company' : data[8].text
               }
        return fund
   
if __name__ == '__main__':
    import sys
    systemHelper.init()
    # with RequestAllFundsTask(fundSpiderContext()) as task:
    #     task.run()
    # with RequestFundsHistoryTask(fundSpiderContext()) as task:
    #    task.run()
    # get Fund Company information
    # with RequestAllCompaniesTask(fundSpiderContext()) as task:
    #    task.run()
       
    # # get Company - Fund information
    # with RequestCompanyToFundTask(fundSpiderContext()) as task:
    #    task.run()
    
    # get Fund - basic information
    with RequestFundBasicInfoTask(fundSpiderContext()) as task:
       task.run()

    # get Manager - Fund data
    # get Fund Manager data
    systemHelper.end()
