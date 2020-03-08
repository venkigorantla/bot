import json
import mysql.connector
import os.path
import time


class MySqlConnector:
    def __init__(self):
        dirpath = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(dirpath, "conndetails.json")
        with open(path) as file:
            conndetails = json.load(file)
            self._host = conndetails["host"]
            self._port = conndetails["port"]
            self._user = conndetails["user"]
            self._password = conndetails["password"]
            self._database = conndetails["database"]

    def connect(self):
        self._db_connection = mysql.connector.connect(user=self._user, password=self._password,
                                                      host=self._host, port=self._port, database=self._database)

    def close(self):
        self._db_connection.close()

    def savecandledata(self, exchange, type, name, duration, items):
        cursor = self._db_connection.cursor()
        query = "INSERT INTO CANDLESTICKDATA (EXCHANGE, TYPE, NAME, DURATION, OPENTIME, CLOSETIME, OPENTIMEUTC, CLOSETIMEUTC, CREATEDTIME, VOLUME, OPEN, CLOSE, HIGH, LOW, EMA7, EMA9) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for item in items:
            opentimeutc = time.strftime("%d %b %Y %H:%M:%S UTC", time.gmtime(int(item["openTime"]) / 1000))
            closetimeutc = time.strftime("%d %b %Y %H:%M:%S UTC", time.gmtime(int(item["closeTime"]) / 1000))
            currenttimeutc = time.strftime("%d %b %Y %H:%M:%S UTC", time.gmtime())
            values = (
            exchange, type, name, duration, item["openTime"], item["closeTime"], opentimeutc, closetimeutc, currenttimeutc,
            item["volume"], item["open"], item["close"], item["high"], item["low"], item["ema7"], item["ema9"])
            cursor.execute(query, values)
            self._db_connection.commit()
        cursor.close()

    def getlatestentry(self, exchange, type, name, duration):
        cursor = self._db_connection.cursor()
        query = "SELECT CLOSETIME, EMA7, EMA9 FROM  CANDLESTICKDATA WHERE EXCHANGE=%s and TYPE=%s and name=%s and DURATION=%s ORDER BY CLOSETIME DESC LIMIT 1"
        cursor.execute(query, (exchange,type, name, duration))
        rows = cursor.fetchall()
        cursor.close()
        if len(rows) > 0:
            return rows[0]
        else:
            return None

    def savetradedata(self, exchange, type, name, data):
        cursor = self._db_connection.cursor()
        query = "INSERT INTO TRADEDATA (EXCHANGE, TYPE, NAME, CURRENTPRICE, TOTALBUYERS, TOTALSELLERS, FRONTLINEBUYERS, FRONTLINESELLERS, TOTALORDERSEXECUTED, MAKERSORDERS, BESTMATCH, CREATEDAT) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        currenttimeutc = time.strftime("%d %b %Y %H:%M:%S UTC", time.gmtime())
        values = (
            exchange, type, name, data["currentprice"], data["totalbuyers"], data["totalsellers"], data["frontlinebuyers"],
            data["frontlinesellers"], data["totalorders"], data["makersorders"], data["bestmatch"], currenttimeutc)
        cursor.execute(query, values)
        self._db_connection.commit()
        cursor.close()
