import io
import json
import glob
import os
import sys
import urllib.request
import sqlite3
import logging
from datetime import *
from bs4 import BeautifulSoup

__all__ = ['spiderHelper',
           'fileHelper',
           'systemHelper',
           'productContext',
           'sqliteHelper']


class productContext:
    DataDirectory = "E:\Work\Data"
    DatabaseFilePath = DataDirectory + "/db/fund.db"


class sqliteHelper:
    import sqlite3

    def connect(dbPath=productContext.DatabaseFilePath):
        try:
            fileHelper.createParentDirectory(dbPath)
            conn = sqlite3.connect(dbPath)
            return conn
        except Exception as e:
            print("Error occurs. dbpath : '{0}'.".format(dbPath))
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

    def saveRequest(url, fileName=None):
        logging.getLogger().debug(url)
        response = urllib.request.urlopen(url)
        html = response.read()
        requestedContent = html.decode('gb2312', "backslashreplace")

        fileHelper.save(spiderHelper._getFilePath(fileName), requestedContent)

        return requestedContent

    def _getFilePath(fileName):
        return "{}/{}".format(productContext.DataDirectory, fileName)


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

    def read(fileName):
        if (fileName != None):
            file = open(fileName,
                        "r", encoding=spiderHelper.FILE_ENCODING)
            content = file.read()
            file.close()
            return content

    def delete(filePattern):
        for f in glob.glob(filePattern):
            os.remove(f)


class systemHelper:
    startTime = datetime.now()
    def init():
        systemHelper.start()
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')
        fileHelper.createDirectory(productContext.DataDirectory)

        systemHelper.initLogger()

    def initLogger():
        logger = logging.getLogger()

        logger.setLevel(logging.INFO)
        # create console handler with a higher log level
        ch = logging.StreamHandler(stream=sys.stdout)
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(levelname)s - %(asctime)s - %(message)s')
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(ch)
   
    def start() :
        startTime = datetime.now()
    
    def end() :
        logging.getLogger().info("Done. during : %s.", (datetime.now() - startTime))
        
