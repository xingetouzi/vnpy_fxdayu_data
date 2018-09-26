from jaqs.data import DataApi
from pymongo import MongoClient
from pymongo.database import Database, Collection
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timedelta
import pandas as pd
import logging
import traceback
from itertools import product
from utils.conf import load
import os


def get_today():
    t = datetime.now()
    return t.year*10000 + t.month*100 + t.day


CONF = {
    "login": {
        "addr": "tcp://data.quantos.org:8910",
        "username": "",
        "password": ""
    },
    "mongodb": {
        "host": "localhost:27017",
        "db": "VnTrader_1Min_Db",
        "log": "log.jqm1"
    },
    "data": {
        "start": 20180101,
        "end": get_today(),
        "symbols": []
    },
    "calendar": "calendar.csv"
}


def get_api():
    login = CONF["login"]
    api = DataApi(login["addr"])
    api.login(login["username"], login["password"])
    return api


def get_mongodb_storage():
    mongodb = CONF["mongodb"]
    log = mongodb["log"].split(".")
    client = MongoClient(mongodb["host"])
    jqindex = MongodbJQIndex(client[log[0]][log[1]])
    writer = MongoDBWriter(client[mongodb["db"]])
    return jqindex, writer

def get_framework():
    data = CONF["data"]
    api = get_api()
    index, writer = get_mongodb_storage()
    return FrameWork(api, index, writer, data["symbols"], CONF["calendar"])


class JQIndex(object):

    def create(self, symbol, date):
        pass
    
    def find(self, symbol, start, end, count=0, insert=0):
        pass
    
    def fill(self, symbol, date, count, insert):
        pass
    
    def latest(self, symbol):
        pass


class Writer(object):

    def write(self, symbol, data):
        pass

    def create(self, symbols):
        pass


def not_empty(item):
    if item[1]:
        return True
    else:
        return False

def join(item):
    return "%s=%s" % item


class FrameWork(object):

    def __init__(self, api, index, writer, symbols, calendar=None):
        assert isinstance(api, DataApi)
        assert isinstance(index, JQIndex)
        assert isinstance(writer, Writer)
        self.api = api
        self.index = index
        self.writer = writer
        self.symbols = symbols
        self.calendar = self.create_calendar(calendar)
        self.writer.create(symbols)

    def query(self, view, fields="", **filters):
        ft = "&".join(map(join, filter(not_empty, filters.items())))
        data, msg = self.api.query(view, ft, fields)
        if msg == "0,":
            return data
        else:
            raise ValueError(msg)

    def get_trade_days(self, start=None, end=None):
        return self.calendar.loc[start:end]
    
    def create_calendar(self, calendar):
        if isinstance(calendar, str) and os.path.isfile(calendar):
            data = pd.read_csv(calendar)["trade_date"]
        elif isinstance(calendar, pd.Series):
            data = calendar
        else:
            data = self.query_trade_days()
        return pd.Series(data.values, data.values).sort_index()

    def query_trade_days(self):
        dates = self.query("jz.secTradeCal")
        return dates["trade_date"].apply(int)
    
    def get_m1_daily(self, symbol, date):
        data, msg = self.api.bar(symbol, trade_date=date)
        if msg == "0,":
            return vnpy_format(data, symbol)
        else:
            raise ValueError(msg)

    def publish(self):
        for symbol, date in self.index.find(count=0):
            self.handle(symbol, date)

    def create(self, symbols=None, start=None, end=None):
        if not symbols:
            symbols = self.symbols
        for symbol in symbols:
            last = self.index.latest(symbol)
            if not last:
               last = start
            for date in self.get_trade_days(last, end):
                self.index.create(symbol, date)

    def handle(self, symbol, date):
        try:
            data = self.get_m1_daily(symbol, date)
        except Exception as e:
            logging.error("query bar | %s | %s | %s", symbol, date, e)
            traceback.print_exc()
            return
        
        if not isinstance(data, pd.DataFrame):
            logging.error("query bar | %s | %s | invalid result: %s", symbol, date, data)
            return
        
        count = len(data)
        if count:
            try:
                insert = self.writer.write(symbol, data)
            except Exception as e:
                logging.error("write bar | %s | %s | %s", symbol, date, e)
                traceback.print_exc()
                return
        else:
            if date < get_today():
                count = -1
            insert = 0
            
        
        try:
            self.index.fill(symbol, date, count, insert)
        except Exception as e:
            logging.error("fill index | %s | %s | %s", symbol, date, e)
            traceback.print_exc()
            return
        else:
            logging.warning("download bar | %s | %s | %s | %s", symbol, date, count, insert)


def vt_symbol(symbol):
    return symbol.replace(".", ":")


BAR_COLUMN = ["vtSymbol", "symbol", "exchange", "open", "high", "low", "close", "date", "time", "datetime", "volume", "openInterest"]

def vnpy_format(data, symbol):
    assert isinstance(data, pd.DataFrame)
    data["datetime"] = list(map(make_time, data["date"], data['time']))
    data["datetime"] = data["datetime"] - timedelta(minutes=1)
    data["date"] = data["date"].apply(str)
    data["vtSymbol"] = vt_symbol(symbol)
    data["symbol"], data["exchange"] = symbol.split(".")
    data["openInterest"] = data["oi"].fillna(0)
    return data[BAR_COLUMN]
    


class MongodbJQIndex(JQIndex):

    SYMBOL = "_s"
    DATE = "_d"
    COUNT = "_c"
    INSERT = "_i"
    MODIFY = "_m"

    def __init__(self, collection):
        assert isinstance(collection, Collection)
        self.collection = collection
        self.collection.create_index([(self.SYMBOL, 1), (self.DATE, 1)], unique=True, background=True)
        self.collection.create_index(self.COUNT)
        self.collection.create_index(self.INSERT)
    
    def create(self, symbol, date):
        doc = {
            self.SYMBOL: symbol,
            self.DATE: int(date),
            self.COUNT: 0,
            self.INSERT: 0,
            self.MODIFY: datetime.now()
        }
        try:
            self.collection.insert_one(doc)
        except DuplicateKeyError:
            logging.warning("create index | %s | %s | duplicated", symbol, date)
        else:
            logging.debug("create index | %s | %s | ok", symbol, date)

    def find(self, symbol=None, start=None, end=None, count=None, insert=None):
        ft = {}
    
        if symbol:
            if isinstance(symbol, list):
                ft[self.SYMBOL] = {"$in": symbol}
            else:
                ft[self.SYMBOL] = symbol
        if start:
            ft[self.DATE] = {"$gte": start}
        if end:
            ft.setdefault(self.DATE, {})["$lte"] = end
        if count is not None:
            ft[self.COUNT] = count
        if insert is not None:
            ft[self.INSERT] = insert
        
        cursor = self.collection.find(ft, [self.SYMBOL, self.DATE])
        for doc in list(cursor):
            yield doc[self.SYMBOL], doc[self.DATE]
        
    def fill(self, symbol, date, count, insert):
        ft = {self.SYMBOL: symbol, self.DATE: date}
        upd = {self.COUNT: count, self.INSERT: insert, self.MODIFY: datetime.now()}
        try:
            result = self.collection.update_one(ft, {"$set": upd})
        except Exception as e:
            logging.error("update index | %s | %s | %s", symbol, date, e)
        else:
            logging.debug("update index | %s | %s | %s", symbol, date, result.modified_count)

    def latest(self, symbol):
        ft = {self.SYMBOL: symbol}
        doc = self.collection.find_one(ft, sort=[(self.DATE, -1)])
        if doc:
            return doc[self.DATE]


class MongoDBWriter(Writer):

    def __init__(self, db):
        assert isinstance(db, Database)
        self.db = db
    
    def create(self, symbols):
        for symbol in symbols:
            self.db[symbol].create_index("datetime", unique=True, background=True)
            self.db[symbol].create_index("date", background=True)
    
    def get_collection(self, symbol):
        return self.db[vt_symbol(symbol)]

    def write(self, symbol, data):
        col = self.get_collection(symbol)
        count = 0
        for doc in data.to_dict("record"):
            try:
                col.insert_one(doc)
            except DuplicateKeyError:
                pass
            else:
                count += 1
        return count
            

def split(num, d=100, left=3):
    while num >= d and (left > 1):
        yield num % d
        num = int(num/d)
        left -= 1
    else:
        for i in range(left):
            yield num
            num = 0


def make_time(date, time):
    day, month, year = tuple(split(date))
    second, minute, hour = tuple(split(time))
    return datetime(year, month, day, hour, minute, second)


def test_index():
    logging.basicConfig(level=logging.DEBUG)
    col = MongoClient("192.168.0.105:37017")["log"]["JQM1"]
    jqindex = MongodbJQIndex(col)
    # jqindex.create("cu.SHF", 20180920)
    for symbol, date in jqindex.find(count=0):
        print(symbol, date)


def test_api():
    data = pd.read_csv("calendar.csv")
    print(data)


def command(filename="conf.yml", commands=None):
    load(filename, CONF)
    fw = get_framework()
    if not commands:
        commands = ["create", "publish"]
    
    for cmd in commands:
        if cmd == "create":
            fw.create(start=CONF["data"]["start"], end=CONF["data"]["end"])
        if cmd == "publish":
            fw.publish()
        

def main():
    import sys
    command(commands=sys.argv[1:])


if __name__ == '__main__':
    main()