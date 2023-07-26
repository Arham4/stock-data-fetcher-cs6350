from collections import OrderedDict
from datetime import datetime, timedelta
from source.source_datastore import GOOGLE_TRENDS_API
import finnhub
import time
import time_utils

ONE_YEAR_FROM_TODAY = 'today 12-m'

class SearchTrendsFetcher:
    def fetch_search_trends(self, source_datastore, symbol, timeframe, source):
        fetcher = get_source(source)
        return fetcher(source_datastore, symbol, timeframe)

def get_source(source):
    if source == GOOGLE_TRENDS_API:
        return _fetch_search_trends_from_google
    else:
        raise ValueError(f'Unknown source: {source}')

def _fetch_search_trends_from_google(source_datastore, symbol, timeframe):
    pytrends = source_datastore.fetch_client(GOOGLE_TRENDS_API)

    kw_list = ["NASDAQ:AAPL"]
    pytrends.build_payload(kw_list, cat=1163, timeframe=timeframe, geo='', gprop='')

    print(f"Fetching search trends for {symbol}...")
    # TODO implement
    return None