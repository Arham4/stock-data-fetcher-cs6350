from source.source_datastore import FINNHUB_API
import finnhub
import time
import time_utils

# The amount of time to sleep in seconds when the API limit is reached
SLEEP_TIME = 60.1

class RSIFetcher:
    def fetch_rsi_values(self, source_datastore, symbol, days, source):
        fetcher = get_source(source)
        return fetcher(source_datastore, symbol, days)

def get_source(source):
    if source == FINNHUB_API:
        return _fetch_rsi_values_from_finnhub
    else:
        raise ValueError(f'Unknown source: {source}')

def _fetch_rsi_values_from_finnhub(source_datastore, symbol, days):
    try:
        finnhub_client = source_datastore.fetch_client(FINNHUB_API)
        to_time = time_utils.get_current_date()
        from_time = time_utils.get_date_for_days_before(to_time, days=days)
        print(f"Fetching RSI values for {symbol}...")
        rsi = finnhub_client.technical_indicator(symbol=symbol, resolution='D', _from=from_time, to=to_time, indicator='rsi', indicator_fields={"timeperiod": 14})
        return rsi
    except finnhub.FinnhubAPIException:
        print(f'API limit reached, waiting {SLEEP_TIME} seconds...')
        time.sleep(SLEEP_TIME)
        return _fetch_rsi_values_from_finnhub(symbol)