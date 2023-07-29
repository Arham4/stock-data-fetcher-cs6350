from source.source_datastore import FINNHUB_API
import finnhub
import time
import time_utils

# The amount of time to sleep in seconds when the API limit is reached
SLEEP_TIME = 60.1

class PriceFetcher:
    def fetch_prices(self, source_datastore, symbol, days, source):
        fetcher = get_source(source)
        return fetcher(source_datastore, symbol, days)

def get_source(source):
    if source == FINNHUB_API:
        return _fetch_prices_from_finnhub
    else:
        raise ValueError(f'Unknown source: {source}')

def _fetch_prices_from_finnhub(source_datastore, symbol, days):
    try:
        finnhub_client = source_datastore.fetch_client(FINNHUB_API)
        to_time = time_utils.get_current_epoch_time()
        from_time = time_utils.get_epoch_time_for_days_before(to_time, days=days)
        print(f"Fetching prices for {symbol}...")
        candles = finnhub_client.stock_candles(symbol, 'D', from_time, to_time)
        return candles
    except finnhub.FinnhubAPIException:
        print(f'API limit reached, waiting {SLEEP_TIME} seconds...')
        time.sleep(SLEEP_TIME)
        return _fetch_prices_from_finnhub(source_datastore, symbol, days)