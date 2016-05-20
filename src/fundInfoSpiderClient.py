
import json
import urllib
import urllib.parse
import urllib.request
import time
import math
import logging
import threading
from concurrent.futures import *
from datetime import date, datetime, timedelta
from helpers import *

__all__ = ["fundInfoSpiderClient"]


class fundInfoSpiderClient:
    host = "http://fund.eastmoney.com"
    all_funds_html_link = "{0}/fund.html#os_0;isall_1;ft_;pt_1"
    all_funds_file = "all_fund_{0}.txt"

    def run(self):
        allFundsFileName = "all_fund_{0}.txt".format(
            date.today().strftime('%Y_%m_%d'))
        self.requestAllFunds(allFundsFileName)

        # get all funds (query, save)
        fileHelper.delete(spiderHelper._getFilePath("fund_*_*.txt"))
        funds = self.readAllFunds(allFundsFileName)
        i = 0
        with ThreadPoolExecutor() as executor:
            for fund in funds:
                try:
                    i += 1
                    fundCode = fund[0]
                    logging.getLogger().info("Request fund %d : '%s'", i, fundCode)
                    future = executor.submit(
                        self.requestFundHistory, fundCode, "fund_{}_{:03d}.txt")
                    future.add_done_callback(threadCallbackHandler(fundCode).handle_callback)
                except Exception as e:
                    logging.getLogger().warn(
                        "Failed {0}. {1}".format(fundCode, e))

        # wait all threads finish.
        executor.shutdown(wait=True)

        # for each fund:
        #   get basic information
        #   get its history information (query, save, store)
        #   get fund managers information
        #   get fund company information
        logging.getLogger().info("Total funds %d", i)

    def getAllFundDataLink(day=date.today()):
        return "{}/Data/Fund_JJJZ_Data.aspx??t=1&lx=1&letter=&gsid=&text=&sort=zdf,desc&page=1,9999&feature=|&dt={}&atfc=&onlySale=0".format(
            fundInfoSpiderClient.host,
            int(time.mktime(day.timetuple())))

    def getFundHistoryDataLink(fundCode, page, count, startDate, endDate):
        return "{}/f10/F10DataApi.aspx?type=lsjz&code={}&page={}&per={}&sdate={}&edate={}&rt=0.17680868120777338".format(fundInfoSpiderClient.host,
                                                                                                                         fundCode, page, count, startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'))

    def getFundBaseInfoLink(fundCode):
        return "{}/f10/jbgk_{}.html".format(fundInfoSpiderClient.host, fundCode)

    def requestAllFunds(self, filename):
        if (fileHelper.exists(filename) == False):
            day = date.today()
            spiderHelper.saveRequest(
                fundInfoSpiderClient.getAllFundDataLink(day), filename)

    def readAllFunds(self, fileName):
        content = fileHelper.read(spiderHelper._getFilePath(fileName))
        fundsString = content[content.find("[["): content.find("]]") + 2]
        funds = eval(fundsString)
        return funds

    def requestFundHistory(self, fundCode, filename, startDate=date(2010, 1, 1), endDate=date.today()):
        page = 1
        content = spiderHelper.saveRequest(
            fundInfoSpiderClient.getFundHistoryDataLink(fundCode,
                                                        page,
                                                        100,
                                                        startDate,
                                                        endDate),
            filename.format(fundCode, page))
        content = content[content.find("{"): content.rfind("}") + 1]
        content = content.replace('content:', '"content":')
        content = content.replace('records:', '"records":')
        content = content.replace('pages:', '"pages":')
        content = content.replace('curpage:', '"curpage":')
        jsonObject = json.loads(content)
        records = int(jsonObject["records"])
        pages = int(jsonObject["pages"]) + 1
        for page in range(2, pages):
            spiderHelper.saveRequest(
                fundInfoSpiderClient.getFundHistoryDataLink(fundCode,
                                                            page,
                                                            100,
                                                            startDate,
                                                            endDate),
                filename.format(fundCode, page))

    def storeAllFunds(self, funds):
        newFunds = []
        for fund in funds:
            newFund = [fund[0], fund[1], date.today(), fund[5], fund[
                6], fund[8], fund[7]]
            newFunds.append(newFund)
        # self.insertFundHistoryData(newFunds)


class threadCallbackHandler:

    def __init__(self, fundCode):
        self.fundCode = fundCode

    def handle_callback(self, future):
        try:
            if (future.exception() != None):
                print("Failed {0}. Exception : {1}".format(
                    self.fundCode, future.exception()))
        except CancelledError as e:
            print("Cancelled {0}".format(self.fundCode))
        print("Done {0}".format(self.fundCode))
        
if __name__ == '__main__':
    import sys
    systemHelper.init()
    client = fundInfoSpiderClient()
    client.run()
    systemHelper.end()
