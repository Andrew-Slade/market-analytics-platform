#need a kafka consumer 
# for each symbol 
# that scrapes the data and turns it into a parquet
# this might actually be best threaded
import logging


class Parquetizer:
    def __init__(self, config_path: str, logger: logging.Logger) -> None:
        pass
    
    def create_consumer_pool(self):
        pass

    def consume_for_symbol(self):
        pass

    def parquetize(self):
        pass