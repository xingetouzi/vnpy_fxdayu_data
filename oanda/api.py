import os
import requests 


ACCOUNTID = os.environ.get("OANDA_ACCOUNTID", "")
REST_PRACTICE = "https://api-fxpractice.oanda.com"
REST_TRADE = "https://api-fxtrade.oanda.com"
STREAM_PRACTICE = "https://stream-fxpractice.oanda.com/"
STREAM_TRADE = "https://stream-fxtrade.oanda.com/"

# CANDLES = "/v3/instruments/{instruments}/candles"
CANDLES = "/v1/candles?instrument={instrument}"
CANDLESV3 = "/v3/instruments/{instrument}/candles"
ACCUOUNTS = "/v3/accounts"
INSTRUMENTS = "/v3/accounts/{accountID}/instruments"
PRICING = "/v3/accounts/{accountID}/pricing?instruments={instruments}"
PRICING_STREAM = "/v3/accounts/{accountID}/pricing/stream?instruments={instruments}"


TRADE = "TRADE"
PRACTICE = "PRACTICE"


def make_url(home, tag, query=None, formats=None):
    if not isinstance(formats, dict):
        formats = {}
    url = home + tag
    url = url.format(**formats)
    if isinstance(query, dict):
        queries = "&".join(["%s=%s" % item for item in query.items()])
        if "?" in url:
            url = "%s&%s" % (url, queries)
        else:
            url = "%s?%s" % (url, queries)
    return url


class OandaAPI(object):

    REST = REST_PRACTICE
    STREAM = STREAM_PRACTICE

    def __init__(self, token, trade_type=PRACTICE):
        self.token = token 
        self.headers = {
            "Authorization": "Bearer %s" % self.token,
            "Content-Type": "application/json"
        }
        if trade_type == TRADE:
            self.REST = REST_TRADE
            self.STREAM = STREAM_TRADE

    def get(self, tag, query=None, **kwargs):
        URL = make_url(self.REST, tag, query, kwargs)
        response = requests.get(URL, headers=self.headers, timeout=20)
        if response.status_code == 200:
            return response.content
        else:
            raise requests.HTTPError(response.status_code, response.content)

    def stream(self, tag, query=None, **kwargs):
        URL = make_url(self.STREAM, tag, query, kwargs)
        response = requests.get(URL, headers=self.headers, stream=True)
        if response.status_code:
            yield from response.iter_lines()
        else:
            raise  requests.HTTPError(response.status_code, response.content)