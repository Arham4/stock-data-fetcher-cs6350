from datetime import datetime, timedelta
import finnhub
import time

# The amount of time to sleep in seconds when the API limit is reached
SLEEP_TIME = 60.1

class RSIFetcher:
    def fetch_rsi_values(self, source_datastore, symbol, days, source):
        fetcher = get_source(source)
        return fetcher(source_datastore, symbol, days)

def get_source(source):
    if source == 'FINNHUB':
        return _fetch_rsi_values_from_finnhub
    else:
        raise ValueError(f'Unknown source: {source}')

def _fetch_rsi_values_from_finnhub(source_datastore, symbol, days):
    try:
        finnhub_client = source_datastore.fetch_client('FINNHUB')
        
        from_time = int((datetime.now() - timedelta(days=days)).timestamp())
        to_time = int(datetime.now().timestamp())
        print(f"Fetching RSI values for {symbol}...")
        rsi = finnhub_client.technical_indicator(symbol=symbol, resolution='D', _from=from_time, to=to_time, indicator='rsi', indicator_fields={"timeperiod": 14})
        return rsi
    except finnhub.FinnhubAPIException:
        print(f'API limit reached, waiting {SLEEP_TIME} seconds...')
        time.sleep(SLEEP_TIME)
        return _fetch_rsi_values_from_finnhub(symbol)