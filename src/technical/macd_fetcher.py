from source.source_datastore import FINNHUB_API
import finnhub
import time
import time_utils
import requests
import logging

# The amount of time to sleep in seconds when the API limit is reached
SLEEP_TIME = 60.1

LOG = logging.getLogger("logger")

class MACDFetcher:
    def fetch_macd_values(self, source_datastore, symbol, days, source):
        fetcher = get_source(source)
        return fetcher(source_datastore, symbol, days)

def get_source(source):
    if source == FINNHUB_API:
        return _fetch_macd_values_from_finnhub
    else:
        raise ValueError(f'Unknown source: {source}')

def _fetch_macd_values_from_finnhub(source_datastore, symbol, days):
    try:
        finnhub_client = source_datastore.fetch_client(FINNHUB_API)
        to_time = time_utils.get_current_epoch_time()
        from_time = time_utils.get_epoch_time_for_days_before(to_time, days=days)
        LOG.info(f"Fetching MACD values for {symbol}...")
        macd = finnhub_client.technical_indicator(symbol=symbol, resolution='D', _from=from_time, to=to_time, indicator='macd', indicator_fields={})
        return macd
    except (finnhub.FinnhubAPIException, requests.exceptions.ReadTimeout) as e:
        LOG.warning(f'API limit reached, waiting {SLEEP_TIME} seconds...', e)
        time.sleep(SLEEP_TIME)
        return _fetch_macd_values_from_finnhub(source_datastore, symbol, days)