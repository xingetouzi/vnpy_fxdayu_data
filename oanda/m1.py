from oanda.api import OandaAPI, CANDLESV3
from datetime import datetime, timedelta, timezone
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from itertools import product
from utils.conf import load
import logging
import json


EXCHANGE = "oanda"


def get_dt(date, tz=None):
    if isinstance(date, int):
        return get_dt(str(date), tz)
    elif isinstance(date, str):
        return datetime.strptime(date.replace("-", ""), "%Y%m%d").replace(tzinfo=tz)
    elif isinstance(date, datetime):
        return date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=tz)
    elif date is None:
        return datetime.now(tz)
    else:
        raise TypeError("Not supported type: %s" % type(date))



class API(OandaAPI):

    def bar(self, instrument, granularity, start, end):
        if isinstance(start, datetime):
            start = start.timestamp()
        if isinstance(end, datetime):
            end = end.timestamp()
        query = {
            "granularity": granularity,
            "from": start,
            "to": end,
        }
        content = self.get(CANDLESV3, query, instrument=instrument)
        data = json.loads(content)

        result = []
        for bar in data["candles"]:
            result.append(self.generate(bar))
        return  result
    
    MAPPER = {"o": "open", "h": "high", "c": "close", "l": "low"}

    def generate(self, bar):
        doc = bar.copy()
        mid = doc.pop("mid")
        for o, t in self.MAPPER.items():
            doc[t] = float(mid[o])
        return doc


def vt_symbol(symbol):
    return "%s:%s" % (symbol, EXCHANGE)


class MongodbStorage(object):

    INSTRUMENT = "_i"
    START = "_s"
    END = "_e"
    DATE = "_d"
    COUNT = "_c"
    FILL = "_f"
    MODIFY = "_m"

    def __init__(self, host=None, db="OANDA_M1", log="log.oanda"):
        self.client = MongoClient(host)
        self.db = self.client[db]
        ldb, lcol = log.split(".", 1)
        self.log = self.client[ldb][lcol]
        self.init_log_collection()

    def ensure_table(self, instrument):
        collection = self.get_collection(instrument)
        info = collection.index_information()
        unique = info.get("datetime_1", {}).get("unique", False)
        if not unique:
            logging.warning("ensure table | %s | index datetime not unique", instrument)
            self.drop_dups(collection)
            collection.create_index("datetime", unique=1, background=True)
            logging.warning("ensure table | %s | unique index datetime created", instrument)
        else:
            logging.debug("ensure table | %s | unique index datetime exists", instrument)
        if "date_1" not in info:
            collection.create_index("date", background=True) 
            logging.warning("ensure table | %s | index date created", instrument)
        else:
            logging.debug("ensure table | %s | index date exists", instrument)
    
    @staticmethod
    def drop_dups(collection):
        dts = set()
        cursor = collection.find(None, ["datetime"])
        _id = None
        while True:
            try:
                doc = next(cursor)
                _id = doc["_id"]
                dt = doc["datetime"]
            except StopIteration:
                break
            except KeyError:
                continue
            except:
                cursor = collection.find({"_id": {"$gte": _id}}, ["datetime"])
            else:
                if dt in dts:
                    collection.delete_one({"_id": _id})
                    logging.warning("drop dups | %s | %s" % (collection, dt))
                else:
                    dts.add(dt)

    def init_log_collection(self):
        self.log.create_index([
            (self.INSTRUMENT, 1),
            (self.DATE, 1)
        ], unique=True, background=True)
    
    def create(self, instrument, date):
        dt = get_dt(date)
        filters = {
            self.INSTRUMENT: instrument,
            self.DATE: date,
        }
        doc = {
            self.START: dt,
            self.END: dt+timedelta(days=1),
            self.COUNT: 0,
            self.FILL: 0,
            self.MODIFY: datetime.now()
        }
        doc.update(filters)
        return self.log.update_one(filters, {"$setOnInsert": doc}, upsert=True).upserted_id
    
    def fill(self, instrument, date, count, fill):
        filters = {
            self.INSTRUMENT: instrument,
            self.DATE: date
        }
        doc = {self.COUNT: count, self.FILL: fill, self.MODIFY: datetime.now()}
        self.log.update_one(filters, {"$set": doc})

    def find(self, instruments=None, start=None, end=None, filled=False):
        filters = {}
        if instruments:
            filters[self.INSTRUMENT] = {"$in": instruments}
        if start:
            filters[self.DATE] = {"$gte": start}
        if end:
            filters.setdefault(self.DATE, {})["$lte"] = end
        if filled:
            filters[self.COUNT] = {"$gte": 0}
        elif filled is not None:
            filters[self.COUNT] = 0
        
        cursor = self.log.find(filters, [self.INSTRUMENT, self.DATE, self.START, self.END])
        for doc in list(cursor):
            yield doc[self.INSTRUMENT], doc[self.DATE], doc[self.START], doc[self.END]

    def time(self, date):
        if isinstance(date, int):
            return self.time(str(date))
        elif isinstance(date, str):
            return datetime.strptime(date.replace("-", ""), "%Y%m%d")
        elif isinstance(datetime, date):
            return date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    def write(self, instrument, data):
        count = 0
        collection = self.get_collection(instrument)
        for bar in data:
            doc = self.vnpy_format(bar, instrument)
            count += self.append(collection, doc)
        return count

    def get_collection(self, instrumet):
        return self.db[vt_symbol(instrumet)]

    def count(self, instrumet, date):
        col = self.get_collection(instrumet)
        return col.find({"date": str(date)}).count()

    @staticmethod
    def append(collection, bar):
        try:
            collection.insert_one(bar)
        except DuplicateKeyError:
            return 0
        else:
            return 1

    def vnpy_format(self, bar, symbol):
        bar.pop("complete", None)
        bar["symbol"] = symbol
        bar["exchange"] = EXCHANGE
        bar["vtSymbol"] = vt_symbol(symbol)
        dt = datetime.strptime(bar.pop("time").split(".")[0], "%Y-%m-%dT%H:%M:%S")
        bar["datetime"] = dt
        bar["date"] = dt.strftime("%Y%m%d")
        bar["time"] = dt.strftime("%H:%M:%S")
        bar["openInterest"] = 0
        return bar
    
    def get_last_date(self, instrument):
        doc = self.log.find_one({self.INSTRUMENT: instrument}, sort=[(self.DATE, -1)])
        if doc:
            return doc[self.DATE]
        else:
            return None


def date_range(start, end=None, tz=None):
    start = get_dt(start, tz)
    end = get_dt(end, tz)
    if end < start:
        return []
    return pd.date_range(start, end).map(lambda t: t.year*10000+t.month*100+t.day)
    


class Framework(object):

    def __init__(self, api, storage):
        assert isinstance(api, API)
        assert isinstance(storage, MongodbStorage)
        self.api = api
        self.storage = storage
        self.tz = timezone(timedelta(hours=0)) 

    def _create(self, instrument, date):
        r = self.storage.create(instrument, date)
        if r:
            logging.debug("create log | %s | %s | %s", instrument, date, r)
            return 1
        else:
            logging.warning("create log | %s | %s | duplicated", instrument, date)
            return 0

    def create(self, instruments, start, end):
        dates = date_range(start, end, self.tz)
        count = 0
        for i, d in product(instruments, dates):
            count += self._create(i, int(d))
        if count > 0:
            logging.warning("create log | %s | %s-%s | %s", instruments, dates[0], dates[-1], count)
        self.ensure(instruments)
        self.check(instruments)

    def update(self, instruments, start, end):
        for i in instruments:
            last = self.storage.get_last_date(i)
            if not last:
                last = start
            dates = date_range(last, end, self.tz)
            count = 0
            for d in dates:
                count += self._create(i, int(d))            
            if count > 0:
                logging.warning("create log | %s | %s-%s | %s", i, dates[0], dates[-1], count)

    def _check(self, instrument, date):
        count = self.storage.count(instrument, date)
        if count > 0:
            self.storage.fill(instrument, date, count, count)
            logging.warning("check | %s | %s | %s", instrument, date, count)

    def check(self, instruments):
        missions = list(self.storage.find(instruments, None, None))
        for i, d, s, e in missions:
            self._check(i, d)

    def ensure(self, instruments):
        for i in instruments:
            self.storage.ensure_table(i)
    
    def publish(self, instruments=None, start=None, end=None, filled=False, redo=3):
        logging.warning("publish cycle start| %s | %s | %s | %s | %s", instruments, start, end, filled, redo)
        now = datetime.now(self.tz)
        missions = list(self.storage.find(instruments, start, end, filled))
        total = len(missions)
        accomplish = 0
        for i, d, s, e in missions:
            s = s.replace(tzinfo=self.tz)
            e = e.replace(tzinfo=self.tz)
            if e >= now:
                logging.warning("publish | %s | %s | end: %s is future", i, d, e)
                accomplish += 1
                continue
            accomplish += self.download(i, d, s, e)
        logging.warning("publish cycle done | total: %s | accomplished: %s", total, accomplish)
        if redo:
            if accomplish < total:
                self.publish(instruments, start, end, False, redo-1)
            
    def download(self, instrument, date, start, end):
        try:
            data = self.api.bar(instrument, "M1", start, end)
            count = len(data)
        except Exception as e:
            logging.error("req bar | %s | %s | %s", instrument, date, e)
            return 0
        
        try:
            if count:
                fill = self.storage.write(instrument, data)
            else:
                fill = 0
                count = -1
        except Exception as e:
            logging.error("write bar | %s | %s | %s", instrument, date, e)
            return 0
        
        try:
            self.storage.fill(instrument, date, count, fill)
        except Exception as e:
            logging.error("fill log | %s | %s | %s", instrument, date, e)
        else:
            logging.warning("download bar | %s | %s | fill: %s, count: %s", instrument, date, fill, count)
            return 1


conf = {
    "oanda": {
        "trade_type": "PRACTICE"
    },
    "mongodb":{
        "host": "localhost:27017"
    },
    "target": {
        "timezone": 0,
        "redo": 3,
        "start": 20180101,
        "instruments": []
    }
}


def command(filename="conf.yml", commands=None):
    load(filename, conf)
    api = API(**conf.get("oanda", {}))
    target = conf["target"]
    storage = MongodbStorage(**conf.get("mongodb", {}))
    fw = Framework(api, storage)
    instruments = target["instruments"]
    
    if len(commands) == 0:
        commands = ["create", "publish"]
    for cmd in commands:
        if cmd == "update":
            fw.update(instruments, target["start"], target.get("end", None))
        elif cmd == "publish":
            fw.publish(instruments, target["start"], target.get("end", None), False, target.get("redo", 3))
        elif cmd == "create":
            fw.create(instruments, target["start"], target.get("end", None))
    

def main():
    import sys
    command(commands=sys.argv[1:])


if __name__ == '__main__':
    main()

