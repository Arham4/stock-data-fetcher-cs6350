from collections import OrderedDict
from datetime import datetime, timedelta
import finnhub
import time
import time_utils

# The amount of time to sleep in seconds when the API limit is reached
SLEEP_TIME = 60.1

class InsiderSentimentFetcher:
    def fetch_insider_sentiment(self, source_datastore, symbol, days, source):
        fetcher = get_source(source)
        return fetcher(source_datastore, symbol, days)

def get_source(source):
    if source == 'FINNHUB':
        return _fetch_insider_sentiment_from_finnhub
    else:
        raise ValueError(f'Unknown source: {source}')

def _fetch_insider_sentiment_from_finnhub(source_datastore, symbol, days):
    finnhub_client = source_datastore.fetch_client('FINNHUB')

    to_date = time_utils.get_current_date()
    from_date = time_utils.get_date_for_days_before(to_date, days)
    print(f"Fetching insider sentiment for {symbol}...")
    market_days = time_utils.get_market_days(from_date, to_date)
    insider_sentiment = _create_insider_sentiment_from_finnhub(finnhub_client, market_days, symbol, from_date, to_date)
    return insider_sentiment
    
def _create_insider_sentiment_from_finnhub(finnhub_client, market_days, symbol, from_date, to_date):
    try:
        insider_sentiment = {}
        finnhub_data = finnhub_client.stock_insider_sentiment(symbol, _from=from_date, to=to_date)['data']
        insider_dict = _create_insider_dict(finnhub_data, market_days)
        for day in market_days:
            split = day.split('-')
            key = split[0] + '-' + split[1]
            insider_sentiment[time_utils.string_to_epoch_in_est(day)] = insider_dict[key]
        return insider_sentiment
    except finnhub.FinnhubAPIException as e:
        print(f'API limit reached, waiting {SLEEP_TIME} seconds...', e)
        time.sleep(SLEEP_TIME)
        return _create_insider_sentiment_from_finnhub(finnhub_client, symbol, from_date, to_date)

def _create_insider_dict(insider_sentiment, market_days):
    def create_key(year, month):
        year = str(year)
        month = str(month)
        if int(month) < 10:
            month = f'0{month}'
        return str(year) + '-' + str(month)

    insider_dict = {}

    if len(insider_sentiment) == 0:
        return {create_key(int(day.split('-')[0]), int(day.split('-')[1])): 0 for day in market_days}

    for sentiment in insider_sentiment:
        key = create_key(sentiment['year'], sentiment['month'])
        insider_dict[key] = sentiment['mspr']

    for day in market_days:
        split = day.split('-')
        year = int(split[0])
        month = int(split[1])

        source_year = year
        source_month = month
        while create_key(source_year, source_month) not in insider_dict:
            if source_month == 1:
                source_month = 12
                source_year -= 1
            else:
                source_month -= 1
        
        insider_dict[create_key(year, month)] = insider_dict[create_key(source_year, source_month)]        
    return insider_dict