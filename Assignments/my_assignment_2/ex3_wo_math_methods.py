from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import datetime
import numpy as np
import sys

class StockTicker(MRJob):

    def filter(self, _, line):
        # Ingest JSON line and validate/cast types
        try:
            row = json.loads(line)
            tick = row['TICK']
            price = float(row['PRICE'])
            ts = datetime.datetime.strptime(row['TS'], "%Y-%m-%d %H:%M:%S.%f")  # validation only
        except:
            return  # Skip malformed rows

        yield tick, price

    def mapper_ticks(self, key, value):
        # Emit disjoint keys per ticker
        yield f"{key}_price", value
        yield f"{key}_count", 1

    def combiner_tick_stats(self, key, values):
        # Aggregate prices or counts locally per map task
        if "_count" in key:
            yield key, sum(values)
        elif "_price" in key:
            prices = list(values)
            yield f"{key}_min", min(prices)
            yield f"{key}_max", max(prices)
            yield f"{key}_sum", sum(prices)

    def reducer(self, key, values):
        # Reorganize keys by ticker
        if "_min" in key:
            yield key.split("_")[0], ('min', min(values))
        elif "_max" in key:
            yield key.split("_")[0], ('max', max(values))
        elif "_sum" in key:
            yield key.split("_")[0], ('sum', sum(values))
        elif "_count" in key:
            yield key.split("_")[0], ('count', sum(values))

    def reducer_aggregate(self, tick, kv_pairs):
        # Recombine the disjoint stats per ticker
        stats = dict(kv_pairs)
        if all(k in stats for k in ['min', 'max', 'sum', 'count']):
            avg = stats['sum'] / stats['count']
            spread = 100 * (stats['max'] - stats['min']) / avg
            result = {
                'min': round(stats['min'], 2),
                'max': round(stats['max'], 2),
                'mean': round(avg, 2),
                'spread': round(spread, 2)
            }

            # print(f"Statistics for {tick}: \n Min Price = {round(result['min'], 2)} \n Max Price = {round(result['max'], 2)} \n Average Price = {round(result['mean'],2)} \n Spread = {round(result['spread'], 2)}", file=sys.stderr)

            yield tick, result

    def steps(self):
        return [
            MRStep(mapper=self.filter),
            MRStep(mapper=self.mapper_ticks,
                   combiner=self.combiner_tick_stats,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_aggregate)
        ]


if __name__ == "__main__":
    StockTicker.run()
