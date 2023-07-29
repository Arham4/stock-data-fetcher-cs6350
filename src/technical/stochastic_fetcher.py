from source.source_datastore import FINNHUB_API
import finnhub
import time
import time_utils

# The amount of time to sleep in seconds when the API limit is reached
SLEEP_TIME = 60.1

class StochasticFetcher:
    def fetch_stochastic_values(self, source_datastore, symbol, days, source):
        fetcher = get_source(source)
        return fetcher(source_datastore, symbol, days)

def get_source(source):
    if source == FINNHUB_API:
        return _fetch_stochastic_values_from_finnhub
    else:
        raise ValueError(f'Unknown source: {source}')

def _fetch_stochastic_values_from_finnhub(source_datastore, symbol, days):
    try:
        finnhub_client = source_datastore.fetch_client(FINNHUB_API)
        to_time = time_utils.get_current_epoch_time()
        from_time = time_utils.get_epoch_time_for_days_before(to_time, days=days)
        print(f"Fetching stochastic values for {symbol}...")
        stochastic = finnhub_client.technical_indicator(symbol=symbol, resolution='D', _from=from_time, to=to_time, indicator='stoch', indicator_fields={"fastkperiod": 14})
        return stochastic
    except finnhub.FinnhubAPIException:
        print(f'API limit reached, waiting {SLEEP_TIME} seconds...')
        time.sleep(SLEEP_TIME)
        return _fetch_stochastic_values_from_finnhub(symbol)