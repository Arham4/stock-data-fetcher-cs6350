from source.source_datastore import FINNHUB_API
import finnhub
import time
import time_utils
import requests
import logging

# The amount of time to sleep in seconds when the API limit is reached
SLEEP_TIME = 60.1

# The FinnHub API only gets data for the past 7 days when doing a social sentiment request
SOCIAL_SENTIMENT_STEP = 7

LOG = logging.getLogger("logger")

class SocialSentimentFetcher:
    def fetch_social_sentiment(self, source_datastore, symbol, days, source):
        fetcher = get_source(source)
        return fetcher(source_datastore, symbol, days)

def get_source(source):
    if source == FINNHUB_API:
        return _fetch_social_sentiment_from_finnhub
    else:
        raise ValueError(f'Unknown source: {source}')

def _fetch_social_sentiment_from_finnhub(source_datastore, symbol, days):
    finnhub_client = source_datastore.fetch_client(FINNHUB_API)

    base_date = time_utils.get_current_date()
    end_date = time_utils.get_date_for_days_before(base_date, days)
    LOG.info(f"Fetching social sentiment for {symbol}...")
    market_days = set(time_utils.get_market_days(end_date, base_date))
    social_sentiment = _traverse_social_sentiment_from_finnhub(finnhub_client, market_days, symbol, base_date, end_date, 
                                                               days=SOCIAL_SENTIMENT_STEP, 
                                                               social_sentiment={'reddit': [], 'twitter': [], 'dates': []})
    return social_sentiment
    
def _traverse_social_sentiment_from_finnhub(finnhub_client, market_days, symbol, base_date, end_date, days, social_sentiment):
    try:
        if time_utils.is_date_before(base_date, end_date) or base_date == end_date:
            return social_sentiment

        to_date = time_utils.get_date_for_days_before(base_date, days)

        if time_utils.is_date_before(to_date, end_date):
            to_date = end_date

        current_sentiment = finnhub_client.stock_social_sentiment(symbol, _from=to_date, to=base_date)
        current_day_by_day = base_date

        while not time_utils.is_date_before(current_day_by_day, to_date):
            reddit_sentiment = [0]
            twitter_sentiment = [0]

            sentiments = current_sentiment['reddit']
            for sentiment in sentiments:
                if current_day_by_day in sentiment['atTime']:
                    score = sentiment['score']
                    reddit_sentiment.append(score)

            sentiments = current_sentiment['twitter']
            for sentiment in sentiments:
                if current_day_by_day in sentiment['atTime']:
                    score = sentiment['score']
                    twitter_sentiment.append(score)
            
            reddit_day_score = sum(reddit_sentiment) / len(reddit_sentiment)
            twitter_day_score = sum(twitter_sentiment) / len(twitter_sentiment)

            if current_day_by_day not in market_days:
                social_sentiment['reddit'][-1] = (social_sentiment['reddit'][-1] + reddit_day_score) / 2
                social_sentiment['twitter'][-1] = (social_sentiment['twitter'][-1] + twitter_day_score) / 2
            else:
                social_sentiment['reddit'].append(reddit_day_score)
                social_sentiment['twitter'].append(twitter_day_score)
                social_sentiment['dates'].append(time_utils.string_to_epoch_in_est(current_day_by_day))

            current_day_by_day = time_utils.get_date_for_days_before(current_day_by_day, 1)

        next_date = time_utils.get_date_for_days_before(to_date, 1)
        return _traverse_social_sentiment_from_finnhub(finnhub_client, market_days, symbol, next_date, end_date, days, social_sentiment)
    except (finnhub.FinnhubAPIException, requests.exceptions.ReadTimeout) as e:
        LOG.warning(f'API limit reached, waiting {SLEEP_TIME} seconds...', e)
        time.sleep(SLEEP_TIME)
        return _traverse_social_sentiment_from_finnhub(finnhub_client, market_days, symbol, base_date, end_date, days, social_sentiment)