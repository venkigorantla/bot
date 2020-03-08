import json
import requests
import os.path
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from framework.candledao import CandleDAO
from framework.indicators import Indicators

class BinanceClient:

    def __init__(self):
        dirpath = os.path.abspath(os.path.dirname(__file__))
        path = os.path.join(dirpath, "config.json")
        with open(path) as file:
            self._configdata = json.load(file)
        self._candledao = CandleDAO()
        self._indicators = Indicators()
        self._exchange = self._configdata["exchange"]
        self._type = self._configdata["type"]
        self._durationlist = self._configdata["candleduration"]

    def ping(self):
        resp = requests.get("https://api.binance.com/api/v1/ping")
        if resp.status_code != 200:
            print("binance ping failed")
        else:
            print("ping successful")

    def collectcandledata(self):
        coins = self.getcoins()
        for coin in coins:
            for duration in self._durationlist:
                candledata = self.getcoincandle(coin, duration)
                candledata = self.filtercandledata(candledata, coin, duration)
                # 7-period EMA calculation
                self._ema7 = self.computeema(candledata, 7, coin, duration)
                # print("7-period EMA = ", self._7periodema15m)
                # 9-period EMA calculation
                self._ema9 = self.computeema(candledata, 9, coin, duration)
                # print("9-period EMA = ", self._9periodema15m)

                # store the data in DB
                self._candledao.storecandledata(self._exchange, self._type, coin, duration,
                                                self.processcandledata(candledata, self._ema7, self._ema9))

    def getcoins(self):
        resp = requests.get("https://api.binance.com/api/v1/ticker/24hr")
        if resp.status_code != 200:
            print("Failed to get 24hr ticker data")
            return None
        else:
            coins = self.filtercoins(resp.json())
            return coins

    def filtercoins(self, resp):
        coins = []
        pairfilter = self._configdata["pairFilter"]
        volthreshold = self._configdata["volumeThreahold"]
        for item in resp:
            volume = float(item["quoteVolume"])
            if volume > volthreshold:
                symbol = item["symbol"]
                if symbol in pairfilter:
                    coins.append(item["symbol"])
        return coins

    def getcoincandle(self, symbol, interval):
        limit = self._configdata["candlelimit"]
        payload = {"symbol": symbol, "interval": interval, "limit": limit}
        resp = requests.get("https://api.binance.com/api/v1/klines", params=payload)
        return resp.json()

    def filtercandledata(self, candledata, name, duration):
        filtereddata = []
        laststoredentry = self.getlatestentryfromdb(name, duration)
        for item in candledata:
            if laststoredentry is not None and item[6] <= laststoredentry[0]:
                continue
            else:
                filtereddata.append(item)
        return filtereddata

    def processcandledata(self, candledata, emadict7=None, emadict9=None):
        output = []
        for item in candledata:
            currenttime = int(round(time.time() * 1000))
            # number secs wait period for candle aggregation
            acceptabletime = currenttime + (int(self._configdata["candlewaittime"]) * 1000)
            open = format(float(item[1]), "0.10f")
            high = format(float(item[2]), "0.10f")
            low = format(float(item[3]), "0.10f")
            close = format(float(item[4]), "0.10f")
            ema7 = None
            ema9 = None
            if emadict7 is not None :
                ema7 = emadict7.get(close)
            if emadict9 is not None:
                ema9 = emadict9.get(close)
            if int(item[6]) <= acceptabletime:
                output.append(
                    {"openTime": item[0], "closeTime": item[6], "volume": item[7], "open": open,
                     "high": high, "low": low, "close": close, "ema7": ema7, "ema9": ema9})
            else:
                print("skipping candle because close time = ", item[6], " not in acceptable window = ", acceptabletime)
        return output

    def computeema(self, candledata, period, name, duration):
        closepricelist = []
        previous_ema = None
        # Read previous EMA from DB
        previousemaentry = self.getlatestentryfromdb(name, duration)
        if previousemaentry is not None:
            if period == 7 and previousemaentry[1] is not None:
                previous_ema = float(previousemaentry[1])
            elif period == 9 and previousemaentry[2] is not None:
                previous_ema = float(previousemaentry[2])
        for item in candledata:
            closepricelist.append(float(item[4]))
        return self._indicators.ema(closepricelist, period, previous_ema)

    def getlatestentryfromdb(self, name, duration):
        # Read previous EMA from DB
        return self._candledao.getlatestentry(self._exchange, self._type, name, duration)

    def getcurrentprice(self, symbol):
        payload = {"symbol": symbol}
        resp = requests.get("https://api.binance.com/api/v3/ticker/price", params=payload)
        jsondata = resp.json()
        return jsondata["price"]

    def getorderdata(self, symbol):
        payload = {"symbol": symbol, "limit": self._configdata["orderdepth"]}
        resp = requests.get("https://api.binance.com/api/v1/depth", params=payload)
        return resp.json()

    def gettradedata(self, symbol):
        payload = {"symbol": symbol}
        resp = requests.get("https://api.binance.com/api/v1/trades", params=payload)
        return resp.json()

    def processtradedata(self, uniquepricelist, tradedata):
        totalorders = 0
        makersmatch = 0
        bestmatch = 0
        for tradeentry in tradedata:
            if tradeentry["price"] in uniquepricelist:
                totalorders = float(tradeentry["qty"]) + totalorders
                if tradeentry["isBuyerMaker"] is True:
                    makersmatch += 1
                if tradeentry["isBestMatch"] is True:
                    bestmatch += 1
        return {"totalorders":totalorders, "buyermaker":makersmatch, "bestmatch": bestmatch}

    def collecttradedata(self):
        for coin in self.getcoins():
            data = {}
            uniquepricepoints = []
            orderdata = self.getorderdata(coin)
            tradedata = self.gettradedata(coin)
            bidsdata = orderdata["bids"]
            asksdata = orderdata["asks"]
            totalbids = 0
            totalasks = 0
            #number of buyer orders with highest price
            frontlinebuyers = bidsdata[0][1]
            #number of seller orders with lowest price
            frontlinesellers = asksdata[0][1]
            for bidentry in bidsdata:
                totalbids = float(bidentry[1]) + totalbids
                uniquepricepoints.append(bidentry[0])
            for askentry in asksdata:
                totalasks = float(askentry[1]) + totalasks
                uniquepricepoints.append(askentry[0])
            tradeoutput = self.processtradedata(uniquepricepoints, tradedata)
            data = {"currentprice": self.getcurrentprice(coin), "totalbuyers": totalbids, "totalsellers": totalasks,
                    "frontlinebuyers": frontlinebuyers, "frontlinesellers": frontlinesellers, "totalorders"
                    :tradeoutput["totalorders"], "makersorders":tradeoutput["buyermaker"], "bestmatch":tradeoutput["bestmatch"]}
            self._candledao.storetradedata(self._exchange, self._type, coin, data)

client = BinanceClient()
client.ping()
client.collectcandledata()
client.collecttradedata()