import io
import json
import os
import sys
import urllib.request
from datetime import *

from bs4 import BeautifulSoup

__all__ = ['fundDataHelper', 'spiderHelper',
           'hostInfoHelper', 'fileHelper', 'systemHelper']


class productContext:
    DataDirectory = "E:\Work\Data"


class restClientHelper:

    def getConnect():
        pass

class fundDataHelper:

    def getFundCodes(fundString):
        fundsArrayString = fundString[
            fundString.find("[["): fundString.find("]]") + 2]
        funds = eval(fundsArrayString)
        return funds

    def getFundBaseInfo():
        #fundManagers = soup.find("th", string="基金经理人").find_parent().find("td").find_all("a")
        # for manager in fundManagers :
        #    print(manager.get('href'))
        #    print(manager.text)
        pass


class spiderHelper:
    FILE_ENCODING = "UTF-8"

    def getSoup(url, fileName=None):
        response = urllib.request.urlopen(url)
        html = response.read()
        soup = BeautifulSoup(html, "html.parser")

        if (fileName != None):
            file = open(spiderHelper._getFilePath(fileName),
                        "w", encoding=spiderHelper.FILE_ENCODING)
            file.write(soup.prettify())
            file.close()

        return soup

    def getSoupFromFile(fileName):
        file = open(spiderHelper._getFilePath(fileName),
                    "r", encoding=spiderHelper.FILE_ENCODING)
        html = file.read()
        file.close()
        soup = BeautifulSoup(html, "html.parser")
        return soup

    def getText(url, fileName=None):
        response = urllib.request.urlopen(url)
        html = response.read()
        jsonStr = html.decode("utf-8")

        if (fileName != None):
            file = open(spiderHelper._getFilePath(fileName),
                        "w", encoding=spiderHelper.FILE_ENCODING)
            file.write(jsonStr)
            file.close()

        return jsonStr

    def _getFilePath(fileName):
        return "{}/{}".format(productContext.DataDirectory, fileName)


class hostInfoHelper:
    hostUrl = "http://fund.eastmoney.com"

    def getAllFundLink():
        return "{}/fund.html#os_0;isall_1;ft_;pt_1".format(hostInfoHelper.hostUrl)
        getAllFundLink = staticmethod(getAllFundLink)

    def getAllFundDataLink():
        return "{0}/Data/Fund_JJJZ_Data.aspx?t=1&lx=0&dt={1}&atfc=&page=1,9999&onlySale=0".format(hostInfoHelper.hostUrl, date.today().strftime('%Y-%m-%d'))
        getAllFundDataLink = staticmethod(getAllFundDataLink)

    def getFundBaseInfoLink(fundCode):
        return "{}/f10/jbgk_{}.html".format(hostInfoHelper.hostUrl, fundCode)
        getFundBaseInfoLink = staticmethod(getFundBaseInfoLink)


class fileHelper:

    def createDirectory(path):
        if not os.path.exists(path):
            os.makedirs(path)


class systemHelper:

    def init():
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')
        fileHelper.createDirectory(productContext.DataDirectory)
