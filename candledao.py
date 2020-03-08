from database.mysql import MySqlConnector

class CandleDAO:

    def __init__(self):
        self._dbconnector = MySqlConnector()
        self._dbconnector.connect()

    def storecandledata(self, exchange, type, name, duration, candledata):
        self._dbconnector.savecandledata(exchange, type, name, duration, candledata)

    def getlatestentry(self, exchange, type, name, duration):
        return self._dbconnector.getlatestentry(exchange, type, name, duration)

    def storetradedata(self, exchange, type, name, data):
        self._dbconnector.savetradedata(exchange, type, name, data)