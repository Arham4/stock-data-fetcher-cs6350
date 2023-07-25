import finnhub
import json

FINNHUB_API = 'FINNHUB'

class SourceDatastore:
    def __init__(self):
        self.cache = {}
        self.config = json.load(open('config.json', mode='r'))

    def fetch_client(self, source):
        if source in self.cache:
            return self.cache[source]
        builder = get_client_builder(source)
        self.cache[source] = builder(self.config)
        return self.cache[source]

def get_client_builder(source):
    if source == FINNHUB_API:
        return _create_finnhub_client
    else:
        raise ValueError(f'Unknown source: {source}')

def _create_finnhub_client(config):
    if 'finnhub_api_key' not in config:
        raise ValueError('Finnhub API key not found in config.json')
    finnhub_client = finnhub.Client(api_key=config['finnhub_api_key'])
    return finnhub_client