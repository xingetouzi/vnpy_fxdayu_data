try:
    from jqdata import jqdata
except ImportError:
    import jqdata
import pandas as pd
from functools import partial
import os
import logging
from pymongo.collection import Collection
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


FILENAME = os.environ.get("JQM1", os.path.join(os.path.dirname(__file__), "conf-constant.yml"))


def get_framework():
    history = jqdata.CONF["history"]
    api = jqdata.get_api()
    index, writer = jqdata.get_mongodb_storage()
    return ConstantFrameWork(api, index, writer, history["symbols"], jqdata.CALENDAR)


def get_mapper_index():
    mongodb = jqdata.CONF["mongodb"]
    mapper = mongodb["mapper"].split(".")
    client = MongoClient(mongodb["host"])
    return MapperIndex(client[mapper[0]][mapper[1]])



class ConstantFrameWork(jqdata.FrameWork):

    def query_info(self):
        data = self.query("jz.instrumentInfo", "delist_date,market")
        data["list_date"] = data["list_date"].apply(int)
        data["delist_date"] = data["delist_date"].apply(int)
        return data

    def create(self, symbols=None, start=None, end=None):
        if not symbols:
            symbols = self.symbols
        if not start:
            start = 0
        if not end:
            end = 99999999
        info = self.query_info()
        info = select(info, start, end)
        for symbol in symbols:
            head, tail = symbol.rsplit(".", 1)
            table = find_match(info, head, tail)
            for index, row in table.iterrows():
                s = row.list_date if row.list_date > start else start
                e = row.delist_date if row.delist_date < end else end
                if s == 0:
                    s = None
                if e == 99999999:
                    e = None
                for date in self.get_trade_days(s, e):
                    self.index.create(row.symbol, date)
            self.writer.create(table.symbol)
            logging.warning("create | %s | %s | %s", symbol, start, end)
            logging.warning("\n%s" % table)

    def find(self, symbols=None, start=None, end=None):
        if not symbols:
            symbols = self.symbols
        if not start:
            start = 0
        if not end:
            end = 99999999
        info = self.query_info()
        info = select(info, start, end)
        trade_dates = self.get_trade_days(start, end, start_shift=-1, end_shift=1)
        for symbol in symbols:
            yield symbol, self._find(symbol, trade_dates, info)
    
    def _find(self, symbol, trade_dates, info):
        head, tail = symbol.rsplit(".", 1)
        table = find_match(info, head, tail)
        _symbols = set(table.symbol)
        data = self.daily(",".join(_symbols), trade_dates[0], trade_dates[-1])
        mc = data.groupby(data.trade_date).apply(find_main_by_oi).reindex(trade_dates).shift(1).dropna()
        mc.name = "symbol"
        mc = mc.reset_index()
        mc["main"] = symbol
        return mc

    def daily(self, symbols, start, end):
        data, msg = self.api.daily(symbols, start, end)
        if msg == "0,":
            return data
        else:
            raise Exception(msg)
    
    def shift_date(self, date, shift=0, side="left"):
        return self.calendar[self.calendar.searchsorted(date, side) + shift]


def find_main_by_oi(frame):
    assert isinstance(frame, pd.DataFrame)
    return frame.loc[frame.oi.idxmax(), "symbol"]


def select(data, start=0, end=99999999):
    assert isinstance(data, pd.DataFrame)
    b1 = data["delist_date"] >= start
    b2 = data["list_date"] <= end
    return data[b1 & b2]


def match(symbol, start, end):
    return symbol.startswith(start) and symbol.endswith(end)


def find_match(data, start, end):
    return data[data.symbol.apply(partial(match, start=start, end=end))]


class MergeFrameWork(object):

    def __init__(self, index, fw):
        assert isinstance(index, MapperIndex)
        assert isinstance(fw, ConstantFrameWork)

        self.index = index
        self.fw = fw
    
    def create(self, symbols, start, end):
        for symbol, mc in self.fw.find(symbols, start, end):
            count = 0
            for idx, row in mc.iterrows():
                count += self.index.create(row.main, row.trade_date, row.symbol)
            logging.warning("create index | %s | %s | %s | %s", symbol, start, end, count)
        self.fw.writer.create(symbols)
    
    def publish(self, symbols, start, end):
        for symbol, date, name in self.index.unfilled(symbols, start, end):
            self.write(symbol, date, name)

    def write(self, symbol, date, name):
        data = self.read_contract(name, date)
        if not len(data):
            logging.warning("read contract | %s | %s | no data", name, date)
            self.index.fill(symbol, date, 0, "no data")
            return

        contract2main(data, symbol)
        data["contract"] = name
        count = self.fw.writer.write(symbol, data)
        self.index.fill(symbol, date, count)
        logging.warning("write main | %s | %s | %s | %s", symbol, name, date, count)

    def read_contract(self, symbol, date):
        _s = date2dt(self.fw.shift_date(date, -1), hour=16)
        _e = date2dt(date, hour=16)
        data = self.fw.writer.read(symbol, _s, _e)
        return data

    def check(self, symbols, start, end):
        for symbol, date, name in self.index.unfilled(symbols, start, end):
            count = self.fw.writer.count(symbol, date)
            if count:
                self.index.fill(symbol, date, count, "check")
                logging.warning("check | %s | %s | %s", symbol, date, count)


def contract2main(data, symbol):
    assert isinstance(data, pd.DataFrame)
    vtSymbol = jqdata.vt_symbol(symbol)
    data["vtSymbol"] = vtSymbol
    data["symbol"] = vtSymbol.rsplit(":", 1)[0]



from datetime import datetime


def date2dt(date, **kwargs):
    return datetime.strptime(str(date), "%Y%m%d").replace(**kwargs)


class MapperIndex(object):

    SYMBOL = "_s"
    DATE = "_d"
    NAME = "_n"
    COUNT = "_c"
    TAG = "_t"

    def __init__(self, collection):
        assert isinstance(collection, Collection)
        self.collection = collection
        self.collection.create_index(
            [
                (self.SYMBOL, 1),
                (self.DATE, 1)
            ],
            unique=True
        )
    
    def create(self, symbol, date, name, tag=""):
        doc = {
            self.SYMBOL: symbol,
            self.DATE: date,
            self.NAME: name,
            self.COUNT: 0,
            self.TAG: tag
        }
        try:
            self.collection.insert_one(doc)
        except DuplicateKeyError:
            return 0
        except Exception as e :
            logging.error("create | %s | %s | %s | %s", symbol, name, date, e)
            return 0
        else:
            logging.debug("create | %s | %s | %s | ok", symbol, name, date)
            return 1
    
    def fill(self, symbol, date, count, tag=""):
        filters = {
            self.SYMBOL: symbol,
            self.DATE: date
        }
        doc = {self.COUNT: count, self.TAG: tag}
        try:
            r = self.collection.update_one(filters, {"$set": doc})
        except Exception as e:
            logging.error("update | %s | %s | %s", symbol, date, e)
            return 0
        else:
            logging.debug("update | %s | %s | ok", symbol, date)
            return r.matched_count

    def unfilled(self, symbol=None, start=None, end=None):
        filters = {self.COUNT: 0}
        if symbol:
            filters[self.SYMBOL] = {"$in": list(symbol)}
        if start:
            filters[self.DATE] = {"$gte": start}
        if end:
            filters.setdefault(self.DATE, {})["$lte"] = end
        for doc in list(self.collection.find(filters)):
            yield doc[self.SYMBOL], doc[self.DATE], doc[self.NAME]
    


def test():
    jqdata.init(r"D:\vnpy_fxdayu_data\jqdata\conf-constant.yml")
    fw = get_framework()
    fw.create(start=20181001, end=20181030)


def command(filename=FILENAME, commands=None):
    jqdata.init(filename)
    histroy = jqdata.CONF["history"]
    fw = get_framework()
    mfw = MergeFrameWork(get_mapper_index(), fw)
    if not commands:
        return
    for cmd in commands:
        if cmd == "create":
            fw.create(histroy["symbols"], histroy["start"], histroy["end"])
        elif cmd == "find":
            mfw.create(histroy["symbols"], histroy["start"], histroy["end"])
        elif cmd == "publish":
            mfw.publish(histroy["symbols"], histroy["start"], histroy["end"])
        elif cmd == "check":
            mfw.check(histroy["symbols"], histroy["start"], histroy["end"])
   

def main():
    import sys
    command(commands=sys.argv[1:])



if __name__ == '__main__':
    main()