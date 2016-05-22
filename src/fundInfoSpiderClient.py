
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

__all__ = ["fundSpiderContext"]

class fundSpiderContext:
    configuration_file = "fund_spider.json"
    host = "http://fund.eastmoney.com"
    all_funds_html_link = "{0}/fund.html#os_0;isall_1;ft_;pt_1"
    all_funds_file = "all_funds_{}.json"
    history_fund_file = "history_{}.json"
    property_last_succeed_request_date = "last_succeed_request_date"
    property_fail_funds = "fail_funds"

    def __init__(self):
        self.confile = os.path.dirname(inspect.getfile(inspect.currentframe())) + "/" + fundSpiderContext.configuration_file
        con = fileHelper.read(self.confile)
        self.configuration = json.loads(con)

    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def close(self):
        con = json.dumps(self.configuration, ensure_ascii=False, indent=4)
        fileHelper.save(self.confile, con)

    def getAllFundsDataLink(day=date.today()):
        return "{}/Data/Fund_JJJZ_Data.aspx??t=1&lx=1&letter=&gsid=&text=&sort=zdf,desc&page=1,9999&feature=|&dt={}&atfc=&onlySale=0".format(
            fundSpiderContext.host,
            int(time.mktime(day.timetuple())))

    def getFundHistoryDataLink(fundCode, page, count, startDate, endDate):
        return "{}/f10/F10DataApi.aspx?type=lsjz&code={}&page={}&per={}&sdate={}&edate={}&rt=0.17680868120777338".format(
            fundSpiderContext.host,
            fundCode, page, count, startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'))

    def getFundBaseInfoLink(fundCode):
        return "{}/f10/jbgk_{}.html".format(fundSpiderContext.host, fundCode)
      
    def getAllFundsFileName():
        return fundSpiderContext.all_funds_file.format(date.today().strftime('%Y_%m_%d'))
        
    def getFundHistoryFileName(fundCode):
        return fundSpiderContext.history_fund_file.format(fundCode)
        
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
                    datetime.strptime(data[0], "%Y-%m-%d").date(),
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
        try:
            if (future.exception() != None):
                logging.getLogger().error("Failed {0}. Exception : {1}".format(
                    self.client_id, future.exception()))
        except CancelledError as e:
            logging.getLogger().error("Cancelled {0}".format(self.client_id))
            
        logging.getLogger().info("Done {0}".format(self.client_id))

class task:
    def __init__(self, context):
        self.context = context
        
    def run(self):
        pass

    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        pass
 
    
class RequestAllFundsTask(task) :
    def __init__(self, context):
        self.context = context

    def run(self):
        self.day = date.today()
        self._requestAllFunds(fundSpiderContext.getAllFundsFileName())

    def _requestAllFunds(self, filename):
        if (fileHelper.exists(filename) == False):
            day = date.today()
            spiderHelper.saveRequest(
                fundSpiderContext.getAllFundsDataLink(day),
                "utf-8",
                filename,
                self.parseAllFundsRequestToJson)

    def parseAllFundsRequestToJson(self, content):
        fundsString = content[content.find("[["): content.find("]]") + 2]
        funds = eval(fundsString)
        fundList = {"date" : self.day.strftime('%Y-%m-%d'),
                    "funds" : []}
        jsonFunds = fundList["funds"]        
        for fund in funds:
            jsonFund = {"code" : fund[0],
                    "name" : fund[1],
                    "pinyin": fund[2]}
            jsonFunds.append(jsonFund)
        return json.dumps(fundList, ensure_ascii=False, indent=4)
                
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        pass
 
class RequestFundsHistoryTask(task):
    def __init__(self, context):
        self.context = context
        self.startDate = context.getProperty(fundSpiderContext.property_last_succeed_request_date)
        if (self.startDate == None) :
            self.startDate = date(2010, 1, 1)
        else:
            self.startDate = datetime.strptime(self.startDate, "%Y-%m-%d").date()
        self.endDate = date.today()
        self.failedFunds = context.getProperty(fundSpiderContext.property_fail_funds)
        if (self.failedFunds == None) :
            self.failedFunds = {}
        
    def run(self):
        funds = []
        if (len(self.failedFunds) == 0):
            allFundsJson = self.readAllFunds(fundSpiderContext.getAllFundsFileName())
            funds = allFundsJson["funds"]
        else:
            for key in self.failedFunds:
                fund = {"code" : key, "name" : self.failedFunds[key]}
                funds.append(fund)
        totalFunds = len(funds)
        self._requestAllFunds(funds)
        
        tryTimes = 0
        while (tryTimes < 3 and len(self.failedFunds) > 0):
            tryTimes += 1
            for key in self.failedFunds:
                fund = {key : self.failedFunds[key]}
                funds.append(fund)
            self._requestAllFunds(funds)

        succeed = False
        if (len(self.failedFunds) == 0) :
            succeed = True
            self.context.setProperty(fundSpiderContext.property_last_succeed_request_date, 
                        self.endDate.strftime('%Y-%m-%d'))
            self.context.setProperty(fundSpiderContext.property_fail_funds, None)
        else:
            self.context.setProperty(fundSpiderContext.property_fail_funds, self.failedFunds)
        self.context.close()
        logging.getLogger().info("%s Total funds %d", "Succeed" if succeed else "Failed", totalFunds)
        
    def _requestAllFunds(self, funds):
        i = 0
        # for fund in funds:
        #     # try:
        #     if (i == 3):
        #         break;
        #     i += 1
        #     fundCode = fund["code"]
        #     fundName = fund["name"]
        #     self.failedFunds[fundCode] = fundName
        #     logging.getLogger().debug("Request fund %d : '%s'", i, fundCode)
        #     self.requestFundHistory( 
        #         fundCode, 
        #         fundName,
        #         fundSpiderContext.getFundHistoryFileName(fundCode))
            # except Exception as e:
            #     logging.getLogger().warn(
            #         "Failed {0}. {1}".format(fundCode, e))
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
        return json.loads(fileHelper.read(spiderHelper.getFilePath(fileName)))

    def requestFundHistory(self, fundCode, fundName, filename):
        page = 1
        content = spiderHelper.saveRequest(
            fundSpiderContext.getFundHistoryDataLink(fundCode,
                                                        page,
                                                        100,
                                                        self.startDate,
                                                        self.endDate),
            "gb2312")
        
        jsonObject = self.getFundHistoryJson(content, fundCode, fundName)
        records = int(jsonObject["records"])
        pages = int(jsonObject["pages"]) + 1
        for page in range(2, pages):
            spiderHelper.saveRequest(
                fundSpiderContext.getFundHistoryDataLink(fundCode,
                                                            page,
                                                            100,
                                                            self.startDate,
                                                            self.endDate),
                "gb2312")
        self.failedFunds.pop(fundCode)
                                                            
    def getFundHistoryJson(self, fundInfo, fundCode, fundName) :
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
                break;
            for td in tds:
                data.append(td.text)
            fund = [fundCode,
                    fundName,
                    datetime.strptime(data[0], "%Y-%m-%d").date(),
                    float(data[1]),
                    float(data[2]),
                    float(data[3][0:data[3].rfind('%')]),
                    None
                    ]
            fundHistory.append(fund)
        return jsonObject

if __name__ == '__main__':
    import sys
    systemHelper.init()
    #with RequestAllFundsTask(fundSpiderContext()) as task:
    #    task.run()
    with RequestFundsHistoryTask(fundSpiderContext()) as task:
        task.run()
    systemHelper.end()
