
import json
import urllib
import urllib.parse
import urllib.request
from helpers import *

__all__ = ["juheClient"]

class juheClient :
    host = "http://japi.juhe.cn/"
    fund_net_value_url = "jingzhi/query.from"
    fund_net_value_appkey = "c48758a6d67b13148911a31fee263d5e"
    fund_net_value_type_zhaiquan = "zhaiquan"
    fund_net_value_type_huobi = "huobi"
    fund_net_value_type_all = "netvalue"
    fund_net_value_type_zhaiquan = "gupiao"
    fund_net_value_type_zhaiquan = "qdii"
    fund_net_value_type_zhaiquan = "hunhe"

    def requestAndSave(self, appUrl, params, fileName, m="GET"):
        paramsInUrl = urllib.parse.urlencode(params)
        if m =="GET":
            f = urllib.request.urlopen("{}{}?{}".format(self.host, appUrl, paramsInUrl))
        else:
            f = urllib.request.urlopen("{}{}".format(self.host, appUrl), paramsInUrl)
            
        content = f.read()
        jsonStr = content.decode("utf-8")
        
        fileHelper.save(spiderHelper._getFilePath(fileName), jsonStr)
        
        return json.loads(jsonStr)
            
    def requestFundNetValue(self, fundtype=fund_net_value_type_all):
        # delete old files
        fileHelper.delete(spiderHelper._getFilePath("fund_net_value_*.json"))
        
        # fetch new files
        page = 1
        error_code = 0
        while (error_code == 0 and page < 1000):
            params = {
                "key" : self.fund_net_value_appkey,
                "pagesize": 20,
                "type": fundtype,
                "page": page
            }
            response = self.requestAndSave(self.fund_net_value_url, params, "fund_net_value_{:03d}.json".format(page))
            if response:
                error_code = response["error_code"]
                result = response["result"]
                if (result == None or len(result) == 0):
                    error_code = 1
            else:
                print("request api error")
                error_code = -1;
            print("Page : {}, ErrorCode: {}".format(page, error_code), flush=True)
            page = page + 1
            
        print("Done.")
     
    def createDB(self):
        sql = '''DROP TABLE IF EXISTS "FundHistory";

                CREATE TABLE "FundHistory" (
                "FundCode" character varying(10) NOT NULL 
                ,"Name" character varying(50) NOT NULL 
                ,"FundDate" date NOT NULL 
                ,"FundType" character varying(50) NOT NULL 
                ,"Price" numeric NOT NULL 
                ,"RealPrice" numeric NOT NULL 
                ,"LastDayPrice" numeric NOT NULL 
                ,"IncrementRate" numeric NOT NULL 
                ,"IncrementValue" numeric NOT NULL 
                ,"FundStatus" character varying(50) NOT NULL 
                ,CONSTRAINT PK_FundHistory PRIMARY KEY (FundCode, FundDate)
                );
                CREATE INDEX IK_FundHistory_FundDate_FundCode ON "FundHistory" (FundDate, FundCode);
                '''
        conn = sqliteHelper.connect()
        try:
            sqliteHelper.executescript(conn, sql)
        except Exception as e:
            print(e)
        finally:
            conn.close()
        
        print("Fund database is created.")
        
    
if __name__ == '__main__':
    import sys
    client = juheClient()
    client.createDB()
