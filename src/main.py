import csv
from stock.stock_fetcher import StockFetcher, LOCAL_SP500, HARDCODED
from source.source_datastore import SourceDatastore, FINNHUB_API
from price.price_fetcher import PriceFetcher
from technical.rsi_fetcher import RSIFetcher
from technical.macd_fetcher import MACDFetcher
from technical.stochastic_fetcher import StochasticFetcher
from sentiment.social_sentiment_fetcher import SocialSentimentFetcher
from sentiment.insider_sentiment_fetcher import InsiderSentimentFetcher

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
SENTIMENT_DAYS = 365

# The average monthly S&P500 stock market returns from 1980 to 2019 were .67% per month
# source: https://stockanalysis.com/article/average-monthly-stock-returns/
AVERAGE_MONTHLY_MARKET_RETURNS = 0.0067

def main():
    stocks_fetcher = StockFetcher()
    source_datastore = SourceDatastore()
    price_fetcher = PriceFetcher()
    rsi_fetcher = RSIFetcher()
    macd_fetcher = MACDFetcher()
    stochastic_fetcher = StochasticFetcher()
    social_sentiment_fetcher = SocialSentimentFetcher()
    insider_sentiment_fetcher = InsiderSentimentFetcher()

    stocks, output_type = stocks_fetcher.fetch_stocks(HARDCODED)

    columns = ['Stock name', 'Date', 'Price', 'RSI', 'MACD', 'Stoch', 'Reddit', 'Twitter', 'Insider', 'Correct']

    with open(f'./output/{output_type}_data.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)

        for symbol in stocks:
            price = price_fetcher.fetch_prices(source_datastore, symbol, days=PRICE_DAYS, source=FINNHUB_API)
            rsi = rsi_fetcher.fetch_rsi_values(source_datastore, symbol, days=TECHNICAL_DAYS, source=FINNHUB_API)
            macd = macd_fetcher.fetch_macd_values(source_datastore, symbol, days=TECHNICAL_DAYS, source=FINNHUB_API)
            stochastic = stochastic_fetcher.fetch_stochastic_values(source_datastore, symbol, days=TECHNICAL_DAYS, source=FINNHUB_API)
            social_sentiment = social_sentiment_fetcher.fetch_social_sentiment(source_datastore, symbol, days=SENTIMENT_DAYS, source=FINNHUB_API)
            insider_sentiment = insider_sentiment_fetcher.fetch_insider_sentiment(source_datastore, symbol, days=SENTIMENT_DAYS, source=FINNHUB_API)

            rsi_dict = {rsi['t'][i]: rsi['rsi'][i] for i in range(len(rsi['rsi']))}
            macd_dict = {macd['t'][i]: macd['macdSignal'][i] for i in range(len(macd['macdSignal']))}
            stochastic_dict = {stochastic['t'][i]: stochastic['slowd'][i] for i in range(len(stochastic['slowd']))}
            reddit_social_sentiment_dict = {social_sentiment['dates'][i]: social_sentiment['reddit'][i] for i in range(len(social_sentiment['reddit']))}
            twitter_social_sentiment_dict = {social_sentiment['dates'][i]: social_sentiment['twitter'][i] for i in range(len(social_sentiment['twitter']))}

            for i in range(DATA_BUFFER, len(price['t'])):
                date = price['t'][i]
                close = price['c'][i]
                rsi_value = rsi_dict.get(date, '')
                macd_value = macd_dict.get(date, '')
                stochastic_value = stochastic_dict.get(date, '')
                reddit = reddit_social_sentiment_dict.get(date, '')
                twitter = twitter_social_sentiment_dict.get(date, '')
                insider = insider_sentiment.get(date, '')

                price_30_days_from_today = price['c'][i + 30] if i + 30 < len(price['c']) else None
                if price_30_days_from_today is None:
                    correct = None
                elif price_30_days_from_today < close * (1 - AVERAGE_MONTHLY_MARKET_RETURNS):
                    correct = -1
                elif price_30_days_from_today >= close * (1 - AVERAGE_MONTHLY_MARKET_RETURNS) and price_30_days_from_today <= close * (1 + AVERAGE_MONTHLY_MARKET_RETURNS):
                    correct = 0
                else:
                    correct = 1

                writer.writerow([symbol, date, close, rsi_value, macd_value, stochastic_value, reddit, twitter, insider, correct])

    print(f"Data fetched and saved to '{output_type}_data.csv' successfully!")

if __name__ == "__main__":
    main()
