import requests

from Schonfeld_task.main.eventHandler import promptType

class ShortQuotaScraper:
    def __init__(self, db_path=r'sqlite:///Schonfeld_task/TWSE_SQ.db'):
        # XXX
        self.sqlDbPath = db_path
        self.sq_url = r'https://mis.twse.com.tw/stock/api/getStockSblsCap.jsp'
        # TODO Websocket implementation

    def requestData(self):
        res = requests.get(self.sq_url)
        if res:
            print(f'{promptType.SYS.value} Successfully extracted data at ')


    def pushFrame(self):
        pass

    def exportFrame(self):
        pass