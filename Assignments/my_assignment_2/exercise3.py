from mrjob.job import MRJob
import json
import datetime
import numpy as np

class StockTicker(MRJob):
    def mapper(self, _, line):
        # ingest json information - make sure that we have all the fields that we need, in the form required
        try:
            row = json.loads(line)
            # now 'row' is a python dictionary with keys: 'TICK', 'PRICE', 'TS'
            # I need to ingest them so as to bring them to their proper data types
            tick = row['TICK']
            price = float(row['PRICE'])  #  we read everything as a string value; need to cast into float to make sure it is valid
            ts = datetime.datetime.strptime(row['TS'], "%Y-%m-%d %H:%M:%S.%f")  
            # I still ingest this even though it is not needed, to make sure
            # that I only work with data coming from lines that are properly configured/formatted
        except:
            return  # ignore invalid lines

        yield tick, price
        # now each mapper will give pairs like ("AMZN", amzn_price1), ("AMZN", amzn_price2), ("TSLA", tesla_price1), ...

    # To be able to work on the results for a single tick, I first need to have gathered all its relevant values somewhere. 
    # Else, if I was to calculate them inside of the mapper functions and yield different min/max/count values for each ticker, 
    # there would be produced a ton of unique keys, making handling each extremely problematic.
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
        
        
        yield key, f"Min Price = {round(item['min'], 2)}. Max Price = {round(item['max'], 2)}. Average Price = {round(item['mean'],2)}. Spread = {round(item['spread'], 2)}"


if __name__ == "__main__":
    StockTicker.run()
