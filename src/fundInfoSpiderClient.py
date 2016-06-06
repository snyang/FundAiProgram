#!/usr/bin/python
# -*- coding: utf-8 -*-
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
import re
from concurrent.futures import *
from datetime import date, datetime, timedelta
from helpers import *
from bs4 import BeautifulSoup
from threading import Lock

__all__ = ["fundSpiderContext", 
            'Task',
            'ConcurrenceTask',
            'TaskManager']

class fundSpiderContext:
    configuration_file = "fund_spider.json"
    host = "http://fund.eastmoney.com"
    all_funds_html_link = "{0}/fund.html#os_0;isall_1;ft_;pt_1"
    property_fail_funds = "fail_funds"
    instance = None
    
    def __init__(self):
        self.confile = os.path.dirname(inspect.getfile(
            inspect.currentframe())) + "/" + fundSpiderContext.configuration_file
        con = fileHelper.read(self.confile)
        self.configuration = json.loads(con)

    def close(self):
        con = json.dumps(self.configuration, ensure_ascii=False, indent=4)
        fileHelper.save(self.confile, con)

    def getInstance():
        if (instance == None):
            instance = fundSpiderContext()
        return instance
        
    def getAllCompaniesLink():
        return "{}/Data/FundRankScale.aspx".format(fundSpiderContext.host)

    def getAllCompaniesFileName():
        return spiderHelper.getFilePath("all_companies.json")

    def getCompanyToFundLink(companyCode):
        return "{}/company/{}.html".format(fundSpiderContext.host, companyCode)

    def getCompanyToFundFileName(companyCode):
        return spiderHelper.getFilePath("/company/company_fund_{}.json".format(companyCode))

    def getAllFundsDataLink(day=date.today()):
        return "{}/Data/Fund_JJJZ_Data.aspx??t=1&lx=1&letter=&gsid=&text=&sort=zdf,desc&page=1,9999&feature=|&dt={}&atfc=&onlySale=0".format(
            fundSpiderContext.host,
            int(time.mktime(day.timetuple())))

    def getAllFundsFileName():
        return spiderHelper.getFilePath("all_funds.json")

    def getAllManagerLink():
        return "{}/Data/FundDataPortfolio_Interface.aspx?dt=14&mc=returnjson&ft=all&pn=10000&pi=1&sc=abbname&st=asc".format(
            fundSpiderContext.host)

    def getAllManagerFileName():
        return spiderHelper.getFilePath("all_managers.json")

    def getFundHistoryDataLink(fundCode, page, count, startDate, endDate):
        return "{}/f10/F10DataApi.aspx?type=lsjz&code={}&page={}&per={}&sdate={}&edate={}&rt=0.17680868120777338".format(
            fundSpiderContext.host,
            fundCode, page, count, typeHelper.toStr(startDate), typeHelper.toStr(endDate))

    def getFundHistoryFileName(fundCode):
        return spiderHelper.getFilePath("/history/history_{}.json".format(fundCode))

    def getFundBasicInfoLink(fundCode):
        return "{}/f10/jbgk_{}.html".format(fundSpiderContext.host, fundCode)

    def getFundBasicInfoFileName(fundCode=None):
        if (fundCode == None):
            return spiderHelper.getFilePath("/funds_info.json")
        return spiderHelper.getFilePath("/fund/fund_{}.json".format(fundCode))

    def getManagerBasicInfoLink(managerCode):
        return "{}/manager/{}.html".format(fundSpiderContext.host, managerCode)

    def getManagerBasicInfoFileName(managerCode=None):
        if (managerCode == None):
            return spiderHelper.getFilePath("/managers_info.json")
        return spiderHelper.getFilePath("/manager/manager_{}".format(managerCode))

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


class Task:
    '''
    Basic task. 
    '''
    def __init__(self, name=None, debug=False):
        '''
        Basic task
        '''
        self.name = name if name != None else type(self).__name__
        self.debug = debug

    def run(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

class ConcurrenceTask(Task):
    '''
    This class supports concurrence task which has multiple steps needed to be run asynchronized
    '''

    def __init__(self, debug=False):
        super().__init__()
        self.lock = Lock()
        self.debug = debug
        self.leftSteps = {}
        self.successfulCount = 0
        self.totalCount = 0

    def prepare(self):
        pass
        
    def getSteps(self):
        '''
        return a dictionay with step key and step data
        '''
        pass

    def runStep(self, stepKey, stepData):
        pass

    def finish(self):
        pass
        
    def run(self):
        self.leftSteps = {}
        self.successfulCount = 0
        self.totalCount = 0
        self.prepare()

        steps = self.getSteps()
        self.totalCount = len(steps)
        self._runTaskSteps(steps)

        retryTimes = 1
        while (retryTimes < 3 and len(self.leftSteps) > 0):
            retryTimes += 1
            self._runTaskSteps(self.leftSteps.copy())

        self.finish()
        logging.getLogger().info("Task %s finished. Success: %d / %d.  Failure: %d",
                                 self.name,
                                 self.successfulCount,
                                 self.totalCount,
                                 len(self.leftSteps))

    def _runTaskSteps(self, steps):
        self.leftSteps = {}
        i = 0
        keys = sorted(steps.keys())
        with ThreadPoolExecutor(max_workers=10) as executor:
            for stepKey in keys:
                try:
                    i += 1
                    if (self.debug and i > 5):
                        break

                    step = steps[stepKey]
                    self.leftSteps[stepKey] = step
                    future = executor.submit(
                        self._runTaskStep, stepKey, step)
                except Exception as e:
                    if (self.debug):
                        raise
                    logging.getLogger().warn("Failed {0}. {1}".format(stepKey, e))

        # wait all threads finish.
        executor.shutdown(wait=True)

    def _runTaskStep(self, stepKey, step):
        try:
            self.leftSteps[stepKey] = step
            self.runStep(stepKey, step)
            self._onSuccess(stepKey)
        except Exception as e:
            logging.getLogger().warn("Failed {0}. {1}".format(stepKey, e))
            logging.getLogger().exception("Failed {0}. Exception : {1}".format(stepKey, e))
 
    def _onSuccess(self, stepKey):
        with self.lock:
            self.leftSteps.pop(stepKey)
            self.successfulCount += 1
            logging.getLogger().info(
                "Done ({}/{}) '{}'".format(self.successfulCount, self.totalCount, stepKey))

class RequestAllFundsTask(Task):

    def run(self):
        self.day = date.today()
        spiderHelper.saveRequest(
            fundSpiderContext.getAllFundsDataLink(self.day),
            "utf-8",
            fundSpiderContext.getAllFundsFileName(),
            self.parseToJson)

    def parseToJson(self, content):
        fundsString = content[content.find("[["): content.find("]]") + 2]
        funds = eval(fundsString)
        fundList = {"date": typeHelper.toStr(self.day),
                    "funds": {}}
        jsonFunds = fundList["funds"]
        for fund in funds:
            jsonFund = {"code": fund[0],
                        "name": fund[1],
                        "pinyin": fund[2]}
            jsonFunds[fund[0]] = jsonFund
        return json.dumps(fundList, ensure_ascii=False, indent=4, sort_keys=True)


class RequestFundsHistoryTask(ConcurrenceTask):

    def prepare(self):
        self.startDate = date(2010, 1, 1)
        self.endDate = date.today() - timedelta(1)
        self.failedFunds = fundSpiderContext.getInstance().getProperty(fundSpiderContext.property_fail_funds)
        if (self.failedFunds == None):
            self.failedFunds = {}
      
    def getSteps(self):
        funds = []
        if (len(self.failedFunds) == 0):
            allFundsJson = fileHelper.readjson(fundSpiderContext.getAllFundsFileName())
            funds = allFundsJson["funds"]
        else:
            funds = self.failedFunds.copy()

        funds = sorted(funds, key=lambda item: item['code'])
        return funds

    def finish(self):
        self.saveConfiguration()
        logging.getLogger().info("Task RequestFundsHistoryTask finish. %s - %s",
                                 typeHelper.toStr(self.startDate),
                                 typeHelper.toStr(self.endDate))

    def saveConfiguration(self):
        if (len(self.failedFunds) == 0):
            self.fundSpiderContext.getInstance().setProperty(fundSpiderContext.property_fail_funds, None)
        else:
            self.fundSpiderContext.getInstance().setProperty(fundSpiderContext.property_fail_funds, self.failedFunds)
        self.fundSpiderContext.getInstance().close()

    def runStep(self, fundCode, fund):
        filename = fundSpiderContext.getFundHistoryFileName(fundCode)
        fundName = fund['name']
        content = ""
        try:
            fileHelper.delete(filename + ".txt")
            fundHistory = {}
            startDate = self.startDate
            endDate = self.endDate
            lastStartDate = None
            lastEndDate = None
            if (fileHelper.exists(filename)):
                fundHistory = fileHelper.readjson(filename)
                lastStartDate = typeHelper.toDate(fundHistory['startDate'])
                lastEndDate = typeHelper.toDate(fundHistory['endDate'])
                startDate = lastEndDate + timedelta(1)
                if (startDate > endDate):
                    self.onSuccess(fundCode)
                    return
            else:
                fundHistory = {'history': []}

            page = 1
            content = spiderHelper.saveRequest(
                fundSpiderContext.getFundHistoryDataLink(fundCode,
                                                         page,
                                                         100,
                                                         startDate,
                                                         endDate),
                "gb2312")

            jsonObject, history = self.getFundHistoryJson(
                content, fundCode, fundName)
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
                jsonObject, newHistory = self.getFundHistoryJson(
                    content, fundCode, fundName)
                history = history + newHistory

            if (len(history) > 0):
                lastEndDate = history[0]['date']
                fundHistory['endDate'] = lastEndDate
                if (lastStartDate == None):
                    lastStartDate = history[len(history) - 1]['date']
                    fundHistory['startDate'] = lastStartDate
                fundHistory['history'] = history + fundHistory['history']
                fileHelper.savejson(filename, fundHistory)

        except Exception as e:
            fileHelper.save(filename + ".txt", content)
            raise
            
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
            realPrice = None  # publish price
            if (len(data[2]) > 0):
                realPrice = float(data[2])
            fund = {'code': fundCode,
                    'name': fundName,
                    'date': data[0],
                    'price': float(data[1]),
                    'realPrice': realPrice,
                    'incrementRate': float(data[3][0:data[3].rfind('%')]) if len(data[3]) > 0 else 0.0
                    }
            fundHistory.append(fund)

        return jsonObject, fundHistory


class RequestAllCompaniesTask(Task):

    def run(self):
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
                       'name': data[1],
                       'pinyin': data[5],
                       'createDate': data[2],
                       'capital': float(data[7]) if len(data[7]) > 0 else None,
                       'manager': data[4],
                       'fundCount': int(data[3]) if len(data[3]) > 0 else 0}
            companies.append(compnay)

        logging.getLogger().info("Found companies %d.", len(companies))
        return json.dumps(companies, ensure_ascii=False, indent=4, sort_keys=True)

class RequestCompanyToFundTask(Task):

    def __init__(self):
        self.code = ""
        self.name = ""
        self.fundCount = 0

    def run(self):
        companies = fileHelper.readjson(
            fundSpiderContext.getAllCompaniesFileName())
        i = 0
        for company in companies:
            i += 1
            if (self.debug and i > 1):
                break
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
                compnayFund = {'companyCode': self.code,
                               'companyName': self.name,
                               'fundCode': fundCode,
                               'fundName': td.a.text}
                companyFundList[fundCode] = compnayFund
        if (self.fundCount == len(companyFundList)):
            logging.getLogger().info("'%s' fund count %d matched.", self.code, self.fundCount)
        else:
            logging.getLogger().error("'%s' fund count %d does not match %d.",
                                      self.code, self.fundCount, len(companyFundList))

        return json.dumps(companyFundList, ensure_ascii=False, indent=4, sort_keys=True)

class RequestFundBasicInfoTask(ConcurrenceTask):

    def __init__(self):
        super().__init__()
        self.fundList = {}

    def prepare(self):
        self.funds = fileHelper.readjson(
            fundSpiderContext.getAllFundsFileName())['funds']
        self.fundList = fileHelper.readjson(
            fundSpiderContext.getFundBasicInfoFileName())
        self.totalCount = len(self.funds)

    def finish(self):
        fileHelper.savejson(
            fundSpiderContext.getFundBasicInfoFileName(), self.fundList)

    def getSteps(self):
        return self.funds

    def runStep(self, fundCode, fund):
        filename = fundSpiderContext.getFundBasicInfoFileName(fundCode)
        content = ""
        try:
            fileHelper.delete(filename + ".txt")
            content = spiderHelper.saveRequest(
                fundSpiderContext.getFundBasicInfoLink(fundCode),
                "gb2312")
            fund = self.parseToJson(content, fundCode)
            self.fundList[fundCode] = fund
        except Exception as e:
            fileHelper.save(filename + ".txt", content)
            raise

    def parseToJson(self, content, fundCode):
        soup = BeautifulSoup(content, "html.parser")
        table = soup.find('table', class_='info w790')

        data = []
        for td in table.find_all('td'):
            data.append(td)
        fund = {
            'code': fundCode,
            'name': data[1].text,
            'fullname': data[0].text,
            'issueDate': data[4].text.replace('年', '-').replace('月', '-').replace('日', ''),
            'companyCode' : self.parseCompanyCode(data[8].a['href']),
            'company': data[8].text,
            'initScale': self.parseInitScale(data[5].text),
            'currentScale': self.parseCurrentScale(data[7].text),
            'capital': self.parseCapital(data[6].text),
            'fundType' : data[3].text,
            'manager': data[10].text
            }
        return fund
    
    def parseCompanyCode(self, data):
        code = re.match('.*?([0-9]+)', data).group(1)
        return code
   
    def parseInitScale(self, data):
        data = data[data.find('/ ') + 2:data.find('亿份')]
        data = float(data) if data != '-' else 0.0
        return data

    def parseCurrentScale(self, data):
        data = data[0:data.find('亿份')]
        data = float(data) if data != '--' else 0.0
        return data

    def parseCapital(self, data):
        data = data[0:data.find('亿元')]
        data = float(data) if data != '--' else 0.0
        return data

class RequestFundManagerTask(Task):

    def __init__(self):
        super().__init__()
        
    def run(self):
        # managers = fileHelper.readjson(fundSpiderContext.getAllManagerFileName())
        spiderHelper.saveRequest(
            fundSpiderContext.getAllManagerLink(),
            "utf-8",
            fundSpiderContext.getAllManagerFileName(),
            self.parseToJson)
        logging.getLogger().info("Task %s finished.", self.name)

    def parseToJson(self, content):
        content = content[content.find("{"): content.rfind("}") + 1]
        content = content.replace('data:', '"data":')
        content = content.replace('record:', '"record":')
        content = content.replace('pages:', '"pages":')
        content = content.replace('curpage:', '"curpage":')
        jsonObject = json.loads(content)
        jsonManagers = jsonObject["data"]
        managers = {}
        for jsonManager in jsonManagers:
            code = jsonManager[0]
            manager = {'code' : code,
                        'name' : jsonManager[1],
                        'companyCode' : jsonManager[2],
                        'companyName' : jsonManager[3],
                        'fundCodes' : jsonManager[4],
                        'fundNames' : jsonManager[5]
                        }
            managers[code] = manager
            
        return json.dumps(managers, ensure_ascii=False, indent=4, sort_keys=True)

class RequestFundManagerDetailTask(ConcurrenceTask):

    def __init__(self):
        super().__init__()
        self.managerList = {}
        
    def prepare(self):
        self.managers = fileHelper.readjson(fundSpiderContext.getAllManagerFileName())
        # self.fundList = fileHelper.readjson(fundSpiderContext.getManagerBasicInfoFileName())
        self.totalCount = len(self.managers)

    def finish(self):
        fileHelper.savejson(
            fundSpiderContext.getManagerBasicInfoFileName(), self.managerList)

    def getSteps(self):
        return self.managers

    def runStep(self, managerCode, m):
        filename = fundSpiderContext.getManagerBasicInfoFileName(managerCode)
        content = ""
        try:
            fileHelper.delete(filename + ".txt")
            content = spiderHelper.saveRequest(
                fundSpiderContext.getManagerBasicInfoLink(managerCode),
                "utf-8")
            manager = self.parseToJson(content, managerCode)
            self.managerList[managerCode] = manager
        except Exception as e:
            fileHelper.save(filename + ".txt", content)
            raise

    def parseToJson(self, content, managerCode):
        soup = BeautifulSoup(content, "html.parser")
        h1 = soup.find('h1', id='jjjl')
        managerName = h1['jlname']
        companyCode = h1['companyid']
        div = soup.find('div', class_='right ms')
        description = div.p.text
        description = re.sub(r'[\\n|\\r]|(基金经理简介：)', '', description).strip()
        div = soup.find('div', class_='right jd ')
        managerInfo = div.text
        managerInfo = managerInfo[managerInfo.find('任职起始日期：') + 7 : managerInfo.find('任职起始日期：') + 7 + 10]
        managerStartDate = typeHelper.toStr(typeHelper.toDate(managerInfo))
        manager = {'code' : managerCode,
                    'name' : managerName,
                    'companyCode' : companyCode,
                    'description' : description,
                    'managerStartDate' : managerStartDate}
                    
        tables = soup.find_all('table', class_='ftrs')
        table = tables[0]
        fundList = []
        for tr in table.tbody.find_all('tr'):
            data = []
            for td in tr.find_all('td'):
                data.append(td)
            if (len(data) < 3):
                continue
            startDate, endDate = self.parseDateRange(data[5].text)
            days = self.parseDays(data[6].text)
            fund = {
                'fundCode': data[0].text,
                'fundName': data[1].text,
                'fundType': data[3].text,
                'fundMoney': float(data[4].text) if data[4].text != '--' else 0.0,
                'startDate': startDate,
                'endDate': endDate,
                'days': days,
                'increment' : float(data[7].text.replace('%', '') if data[7].text != '-' else 0.0)
            }
            fundList.append(fund)
        manager['fundList'] = fundList
        
        table = tables[1]
        fundPerformanceList = []
        for tr in table.tbody.find_all('tr'):
            data = []
            for td in tr.find_all('td'):
                data.append(td)
            if (len(data) < 3):
                continue
            fund = {
                'fundCode': data[0].text,
                'fundName': data[1].text,
                'fundType': data[2].text,
                'threeMonthRate': float(data[3].text.replace('%', '')) if len(data[3].text) > 1 else 0.0,
                'threeMonthRank': int(data[4].text.split('|')[0]) if data[4].text.split('|')[0] != '-' else None,
                'threeMonthTotal': int(data[4].text.split('|')[1]) if data[4].text.split('|')[1] != '-' else None,
                'sixMonthRate': float(data[5].text.replace('%', '')) if len(data[5].text) > 1 else 0.0,
                'sixMonthRank': int(data[6].text.split('|')[0]) if data[6].text.split('|')[0] != '-' else None,
                'sixMonthTotal': int(data[6].text.split('|')[1]) if data[6].text.split('|')[1] != '-' else None,
                'oneYearRate': float(data[7].text.replace('%', '')) if len(data[7].text) > 1 else 0.0,
                'oneYearRank': int(data[8].text.split('|')[0]) if data[8].text.split('|')[0] != '-' else None,
                'oneYearTotal': int(data[8].text.split('|')[1]) if data[8].text.split('|')[1] != '-' else None,
                'twoYearRate': float(data[9].text.replace('%', '')) if len(data[9].text) > 1 else 0.0,
                'twoYearRank': int(data[10].text.split('|')[0]) if data[10].text.split('|')[0] != '-' else None,
                'twoYearTotal': int(data[10].text.split('|')[1]) if data[10].text.split('|')[1] != '-' else None,
                'thisYearRate': float(data[11].text.replace('%', '')) if len(data[11].text) > 1 else 0.0,
                'thisYearRank': int(data[12].text.split('|')[0]) if data[12].text.split('|')[0] != '-' else None,
                'thisYearTotal': int(data[12].text.split('|')[1]) if data[12].text.split('|')[1] != '-' else None
            }
            fundPerformanceList.append(fund)
        manager['fundPerformanceList'] = fundPerformanceList
            
        return manager
        
    def parseDateRange(self, dateRange) :
        dates = dateRange.split("~")
        startDate = typeHelper.toStr(typeHelper.toDate(dates[0].strip()))
        endDate = None
        try:
            endDate = typeHelper.toStr(typeHelper.toDate(dates[1].strip()))
        except Exception as e:
            pass
        return startDate, endDate

    def parseDays(self, daysText) :
        years = 0
        days = 0
        if (len(daysText) == 0):
            return 0
        if (daysText.find('年') > -1) :
            years = int(daysText[0 : daysText.find('年')])
            daysText = daysText[daysText.find('年') : len(daysText)]
 
        days = int(re.match('.*?([0-9]+)', daysText).group(1))
        return years * 365 + days
        
class TaskManager(Task):
    '''
    This class is used to manager multiple tasks.
    '''

    def __init__(self, tasks, debug=False):
        super().__init__(debug=debug)
        self.tasks = tasks

    def run(self):
        for task in self.tasks:
            task.debug = self.debug
            task.run()
            logging.getLogger().info("Task %s finished.", task.name)

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
    # with RequestAllFundsTask() as task:
    #     task.run()
    # with TaskManager([RequestFundManagerTask()]) as task:
    #     task.run()

    # with TaskManager([RequestFundBasicInfoTask()]) as task:
    #     task.run()
        
    # with TaskManager([RequestFundManagerDetailTask()]) as task:
    #     task.run()
        
    # get Manager - Fund data
    # get Fund Manager data
    systemHelper.end()
