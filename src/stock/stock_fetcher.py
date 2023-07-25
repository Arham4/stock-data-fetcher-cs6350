LOCAL_SP500 = 'LOCAL_S&P500'
HARDCODED = 'HARDCODED'

class StockFetcher:
    def fetch_stocks(self, source):
        fetcher = get_source(source)
        return fetcher()

def get_source(source):
    if source == 'LOCAL_S&P500':
        return _fetch_local_sp500_stocks
    elif source == 'HARDCODED':
        return _fetch_hardcoded_stocks
    else:
        raise ValueError(f'Unknown source: {source}')

def _fetch_local_sp500_stocks():
    stocks = []
    for line in open('../../data/s&p500_stock_names.txt', mode='r').read().splitlines():
        stocks.append(line.split(',')[0])
    return stocks, 'sp500'

def _fetch_hardcoded_stocks():
    return ['AAPL'], 'hardcoded'