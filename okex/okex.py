import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from utils.conf import load
from utils.mongodb import update
import os
import logging


FILENAME = os.environ.get("BINANCE", os.path.join(os.path.dirname(__file__), "conf.yml"))


SPOT_KLINE = "https://www.okex.com/api/v1/kline.do"
FUTURE_KLINE = "https://www.okex.com/api/v1/future_kline.do"
HEADERS = {
    "contentType": "application/x-www-form-urlencoded"
}

REQ_ARGS = {
    "headers": HEADERS
}
SPOT_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
FUTURE_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume", "t_volume"]

EXCHANGE = "OKEX"

CONF = {
    "mongodb": {
        "host": "localhost",
        "log": "log.okex_v1",
        "db": "VnTrader_1Min_Db"
    },
    "target": {
        "futures": [],
        "spots": []
    }
}


def init(filename=FILENAME):
    load(filename, CONF)
    if "proxies" in CONF:
        REQ_ARGS["proxies"] = CONF["proxies"]


def vt_symbol(symbol):
    return "%s:%s" % (symbol, EXCHANGE)


def get(url):
    rsp = requests.get(url, **REQ_ARGS)
    if rsp.status_code == 200:
        return json.loads(rsp.content)
    else:
        raise Exception(rsp.status_code, rsp.content)


def join_params(**kwargs):
    return "&".join(["%s=%s" % item for item in kwargs.items()])


def get_url(head, **kwargs):
    return "%s?%s" % (head, join_params(**kwargs))


def future_kline_url(symbol, type, contract_type, size=0, since=0):
    kwargs = {}
    kwargs["symbol"] = symbol
    kwargs["type"] = type
    kwargs["contract_type"] = contract_type
    if size:
        kwargs["size"] = size
    if since:
        kwargs["since"] = since
    return get_url(FUTURE_KLINE, **kwargs)


def spot_kline_url(symbol, type, size=0, since=0):
    kwargs = {}
    kwargs["symbol"] = symbol
    kwargs["type"] = type
    if size:
        kwargs["size"] = size
    if since:
        kwargs["since"] = since
    return get_url(SPOT_KLINE, **kwargs)


def get_future_kline(symbol, type, contract_type, start):
    url = future_kline_url(symbol, type, contract_type, since=dt2mts(start) if start else 0)
    docs = get(url)
    if len(docs) > 1:
        frame = pd.DataFrame(docs[:-1], columns=FUTURE_COLUMNS)
        return frame
    else:
        raise ValueError("Data not enough.")


def get_spot_kline(symbol, type, start):
    url = spot_kline_url(symbol, type, since=dt2mts(start) if start else 0)
    docs = get(url)
    if len(docs) > 1:
        frame = pd.DataFrame(docs[:-1], columns=SPOT_COLUMNS)
        return frame
    else:
        raise ValueError("Data not enough.")


def vnpy_format(frame, symbol):
    assert isinstance(frame, pd.DataFrame)
    frame["datetime"] = frame.pop("timestamp").apply(mts2dt)
    frame["date"] = frame["datetime"].apply(dt2date)
    frame["time"] = frame["datetime"].apply(dt2time)
    frame["exchange"] = EXCHANGE
    frame["symbol"] = symbol
    frame["vtSymbol"] = vt_symbol(symbol)
    frame["openInterest"] = 0
    for name in ["open", "high", "low", "close", "volume"]:
        frame[name] = frame[name].apply(float)


def vnpy_future_1min(symbol, start=None):
    s, c = symbol.split("_", 1)
    data = get_future_kline(s, "1min", c, start)
    data.pop("t_volume")
    vnpy_format(data, symbol)
    return data


def vnpy_spot_1min(symbol, start=None):
    data = get_spot_kline(symbol, "1min", start)
    vnpy_format(data, symbol)
    return data


def dt2time(dt):
    return dt.strftime("%H:%M:%S")


def dt2date(dt):
    return dt.strftime("%Y%m%d")


def mts2dt(ts):
    return datetime.fromtimestamp(int(ts/1000))


def dt2mts(dt):
    return int(dt.timestamp()*1000)


def write(db, log, symbol, method):
    now =  datetime.now()
    try:
        data = method(symbol)
    except Exception as e:
        logging.error("query 1min data | %s | %s", symbol, e)
        return
    col = db[vt_symbol(symbol)]
    try:
        match, upsert = update(col, data.set_index("datetime"))
    except Exception as e:
        logging.error("write db | %s | %s", symbol, e)
        return

    write_log(log, symbol, now, upsert, match)
    logging.warning("update 1min | %s | %s | match=%s | upsert=%s", symbol, now, match, upsert)
    

def write_log(col, symbol, time, upsert=0, matched=0):
    ft = {
        "_s": symbol,
        "_t": time,
        "_u": upsert,
        "_m": matched
    }
    try:
        col.insert_one(ft)
    except DuplicateKeyError:
        pass
    else:
        logging.debug("write log | %s | %s | match=%s | upsert=%s", symbol, time, matched, upsert)


def get_storage():
    mongodb = CONF["mongodb"]
    host = mongodb["host"]
    log_db, log_col = mongodb["log"].split(".", 1)
    client = MongoClient(host)
    db = client[mongodb["db"]]
    log = client[log_db][log_col] 
    return db, log


def create():
    db, log = get_storage()
    log.create_index([("_s", 1), ("_t", 1)])
    for future in CONF["target"]["futures"]:
        create_table_index(db[vt_symbol(future)])
    for spot in CONF["target"]["spots"]:
        create_table_index(db[vt_symbol(spot)])


def publish():
    db, log = get_storage()
    for future in CONF["target"]["futures"]:
        write(db, log, future, vnpy_future_1min)
    for spot in CONF["target"]["spots"]:
        write(db, log, spot, vnpy_spot_1min)


def create_table_index(collection):
    collection.create_index("datetime", unique=True, background=True)
    collection.create_index("date", background=True)
    logging.warning("create index | %s", collection.full_name)


def run(*commands):
    init()
    for command in commands:
        if command == "create":
            create()
        elif command == "publish":
            publish()


def main():
    import sys
    commands = sys.argv[1:]
    run(*commands)


if __name__ == '__main__':
    main()