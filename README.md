# Stock Data Fetcher

This project fetches data from the stock market. It has built in a way to be flexible enough to support multiple datasources. Currently, only FinnHub is used to fetch data from.

This was made for CS 6350 (Big Data Management and Analytics) at the University of Texas at Dallas, taught by Dr. Anurag Nagar.

## Setup

Python 3.11.4 was used to run the code. A `requirements.txt` file is provided for dependencies. Run the following command to install them:

```bash
pip install -r requirements.txt
```

A `config.json` file must also be made. In it, API keys can be found.
```json
{
  "finnhub_api_key": "your finnhub api key"
}
```