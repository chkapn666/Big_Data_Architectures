from mrjob.job import MRJob
import json
import datetime
import numpy as np

class StockTicker(MRJob):
    def mapper(self, _, line):
        # ingest json information
        try:
            row = json.loads(line)
            # now 'row' is a python dictionary with keys: 'TICK', 'PRICE', 'TS'
            # I need to ingest them so as to bring them to their proper data types
            tick = row['TICK']
            price = float(row['PRICE'])  # 🔧 cast to float
            ts = datetime.datetime.strptime(row['TS'], "%Y-%m-%d %H:%M:%S.%f")  
            # I still ingest this even though it is not needed, to make sure
            # that I only work with data coming from lines that are properly configured/formatted
        except:
            return  # ignore invalid lines

        yield tick, price
        # now each mapper will give pairs like ("AMZN", amzn_price1), ("AMZN", amzn_price2), ("TSLA", tesla_price1), ...

    # so the reducer will receive one key-value pair per tick, like 
    # ("AMZN", [amzn_price1, amzn_price2, ...])
    def reducer(self, key, values):
        prices = list(values)  # 'values' comes as a generator object by default - need to convert to list to accommodate my desired operations
        item = {
            'min': np.min(prices),
            'max': np.max(prices),
            'mean': np.mean(prices),
            'spread': 100 * (np.max(prices) - np.min(prices)) / np.mean(prices)
        }
        
        print(f"Statistics for {key}: \n Min Price = {round(item['min'], 2)} \n Max Price = {round(item['max'], 2)} \n Average Price = {round(item['mean'],2)} \n Spread = {round(item['spread'], 2)}")
        
        yield key, item


if __name__ == "__main__":
    StockTicker.run()
