import io
import json
import glob
import os
import sys
import urllib.request
import sqlite3
from datetime import *

from bs4 import BeautifulSoup

__all__ = ['fundDataHelper', 'spiderHelper',
           'hostInfoHelper', 'fileHelper', 'systemHelper',
           'productContext',
           'sqliteHelper']


class productContext:
    DataDirectory = "E:\Work\Data"
    DatabaseFilePath = DataDirectory + "/db/fund.db"


class sqliteHelper:
    import sqlite3

    def connect(dbPath = productContext.DatabaseFilePath):
        try:
            fileHelper.createParentDirectory(dbPath)
            conn = sqlite3.connect(dbPath)
            return conn
        except Exception as e:
            print("Error occurs. dbpath : '{}'.".format(dbPath))
            raise

    def close(conn):
        conn.close()

    def execute(conn, sql):
        cur = None
        with conn:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        return cur

    def executemany(conn, sql, params):
        cur = None
        with conn:
            cur = conn.cursor()
            cur.executemany(sql, params)
            conn.commit()  
        return cur

    def executescript(conn, sql):
        cur = None
        with conn:
            cur = conn.cursor()
            cur.executescript(sql)
            conn.commit()  
        return cur
        
class fundDataHelper:

    def getFundCodes(fundString):
        fundsArrayString = fundString[
            fundString.find("[["): fundString.find("]]") + 2]
        funds = eval(fundsArrayString)
        return funds

    def getFundBaseInfo():
        # fundManagers = soup.find("th", string="基金经理人").find_parent().find("td").find_all("a")
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

        fileHelper.save(spiderHelper._getFilePath(fileName), jsonStr)

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

    def createParentDirectory(path):
        fileHelper.createDirectory(os.path.dirname(path))
        
    def createDirectory(path):
        if not os.path.exists(path):
            os.makedirs(path)

    def exists(path):
        return os.path.exists(path)
        
    def save(fileName, content):
        if (fileName != None):
            file = open(fileName,
                        "w", encoding=spiderHelper.FILE_ENCODING)
            file.write(content)
            file.close()

    def delete(filePattern):
        for f in glob.glob(filePattern):
            os.remove(f)


class systemHelper:

    def init():
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')
        fileHelper.createDirectory(productContext.DataDirectory)
