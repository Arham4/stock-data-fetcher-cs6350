import csv
from stock.stock_fetcher import StockFetcher, LOCAL_SP500, HARDCODED
from source.source_datastore import SourceDatastore, FINNHUB_API
from price.price_fetcher import PriceFetcher
from technical.rsi_fetcher import RSIFetcher
from technical.macd_fetcher import MACDFetcher
from technical.stochastic_fetcher import StochasticFetcher
from sentiment.social_sentiment_fetcher import SocialSentimentFetcher

# The amount of days we want to fetch data for
PRICE_DAYS = 455

# The amount of buffer room the technical indicators need in order to calculate values from the previous day pricepoints
TECHNICAL_BUFFER = 42

# The amount of buffer in the data to ignore when dumping data in order to guarantee all data being present
DATA_BUFFER = 61

# The amount of days of technical data we want to fetch data for
TECHNICAL_DAYS = PRICE_DAYS - TECHNICAL_BUFFER

# The amount of days of social sentiment data we want to fetch data for. Since the social sentiment data is available every day, including non-trading days,
# the required time horizon is less
SOCIAL_SENTIMENT_DAYS = 365

def main():
    stocks_fetcher = StockFetcher()
    source_datastore = SourceDatastore()
    price_fetcher = PriceFetcher()
    rsi_fetcher = RSIFetcher()
    macd_fetcher = MACDFetcher()
    stochastic_fetcher = StochasticFetcher()
    social_sentiment_fetcher = SocialSentimentFetcher()

    stocks, output_type = stocks_fetcher.fetch_stocks(HARDCODED)

    columns = ['Stock name', 'Date', 'Price', 'RSI', 'MACD', 'Stoch', 'Reddit', 'Twitter']

    with open(f'./output/{output_type}_data.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)

        for symbol in stocks:
            price = price_fetcher.fetch_prices(source_datastore, symbol, days=PRICE_DAYS, source=FINNHUB_API)
            rsi = rsi_fetcher.fetch_rsi_values(source_datastore, symbol, days=TECHNICAL_DAYS, source=FINNHUB_API)
            macd = macd_fetcher.fetch_macd_values(source_datastore, symbol, days=TECHNICAL_DAYS, source=FINNHUB_API)
            stochastic = stochastic_fetcher.fetch_stochastic_values(source_datastore, symbol, days=TECHNICAL_DAYS, source=FINNHUB_API)
            social_sentiment = social_sentiment_fetcher.fetch_social_sentiment(source_datastore, symbol, days=SOCIAL_SENTIMENT_DAYS, source=FINNHUB_API)

            rsi_dict = {rsi['t'][i]: rsi['rsi'][i] for i in range(len(rsi['rsi']))}
            macd_dict = {macd['t'][i]: macd['macdSignal'][i] for i in range(len(macd['macdSignal']))}
            stochastic_dict = {stochastic['t'][i]: stochastic['slowd'][i] for i in range(len(stochastic['slowd']))}
            reddit_social_sentiment_dict = {social_sentiment['dates'][i]: social_sentiment['reddit'][i] for i in range(len(social_sentiment['reddit']))}
            twitter_social_sentiment_dict = {social_sentiment['dates'][i]: social_sentiment['twitter'][i] for i in range(len(social_sentiment['twitter']))}

            for i in range(len(price['t'])):
                if i < DATA_BUFFER:
                    continue

                date = price['t'][i]
                close = price['c'][i]
                rsi_value = rsi_dict.get(date, '')
                macd_value = macd_dict.get(date, '')
                stochastic_value = stochastic_dict.get(date, '')
                reddit = reddit_social_sentiment_dict.get(date, '')
                twitter = twitter_social_sentiment_dict.get(date, '')

                writer.writerow([symbol, date, close, rsi_value, macd_value, stochastic_value, reddit, twitter])

    print(f"Data fetched and saved to '{output_type}_data.csv' successfully!")

if __name__ == "__main__":
    main()
