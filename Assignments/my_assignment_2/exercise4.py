from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq
import datetime

class EarthquakeAnalyzer(MRJob):

    def mapper_init(self):
        self.all_records = []

    def mapper(self, _, line):
        # Each mapper is assigned a particular portion of a file, which is expressed as a particular group of lines. 
        # Each mapper will parse their delegated portion line by line. 
        # I need to make sure that every line parsed comes in the proper form (aka has all the required fields and in the correct format), or I ignore it.
        row = line.strip().split(',')
        try:
            ts = datetime.datetime.strptime(row[0], "%m/%d/%Y %H:%M")
            latitude = float(row[1])
            longitude = float(row[2])
            depth = int(row[3])
            magnitude = float(row[4])

            self.all_records.append((magnitude, ts, latitude, longitude, depth))

        except:
            return  # Ignore bad lines

    def mapper_final(self):
        # So each mapper first parses the entirety of lines delegated to it, and saves all relevant earthquake records 
        # into a list. It then iterates through the list to extract all the useful information. 
        top10 = heapq.nlargest(10, self.all_records, key=lambda x: x[0])  # this way, each mapper finds its top 10 earthquakes
        athens_records = []

        for quake in self.all_records:  # checking every single detected record for the desired attributes
            magnitude, ts, lat, lon, _ = quake

            # Q2: Count quakes per year/month after 2010
            if ts.year > 2010:
                yield (f"q2_{ts.year}_{ts.month}"), 1

            # Q3: Mag stats between 2010–2020 inclusive
            if 2010 <= ts.year <= 2020:
                yield (f"q3_{ts.year}"), magnitude

            # Q4: Check if in Athens bounds
            if 37.5 <= lat <= 39.0 and 23.35 <= lon <= 23.55:
                athens_records.append((magnitude, ts.strftime("%Y-%m-%d %H:%M")))
            # !!! MRJob tries to serialize everything with JSON, but datetime.datetime objects aren’t JSON serializable by default.
            # Convert any datetime object to a string (e.g., strftime() or isoformat()) before yielding from any MRJob phase (mapper or reducer).


        # Q1: Emit top-10 overall (with timestamp formatted)
        for magnitude, ts, lat, lon, depth in top10:
            yield "q1", (magnitude, ts.strftime("%Y-%m-%d %H:%M"), lat, lon, depth)  # datetime converted to string to communicate to the reducer

        # Q4: Emit possible Athens top 5
        top5_athens = heapq.nlargest(5, athens_records, key=lambda x: x[0])
        for quake in top5_athens:
            yield "q4", quake


    def reducer(self, key, values):
        if key == "q1":
            # values are tuples like (magnitude, "YYYY-MM-DD HH:MM", lat, lon, depth)
            top10 = heapq.nlargest(10, values, key=lambda x: x[0])  # i find the top 10 earthquakes from the combined results of all mappers
            for magnitude, ts_str, *_ in top10:
                # `datetime` values have already been converted to string to be transferred to the reducer, because else they are not serializable by default
                date_str, time_str = ts_str.split()
                yield "Top10_Greece", (date_str, time_str, magnitude)

        elif key.startswith("q2_"):
            yield f"Earthquakes in {key[3:].replace('_', '-')}", sum(values)

        elif key.startswith("q3_"):
            values = list(values)
            year = key[3:]
            yield f"{year}_Min_Magnitude", min(values)
            yield f"{year}_Max_Magnitude", max(values)
            yield f"{year}_Avg_Magnitude", round(sum(values) / len(values), 2)

        elif key == "q4":
            # values are (magnitude, "YYYY-MM-DD HH:MM")
            top5 = heapq.nlargest(5, values, key=lambda x: x[0])
            for magnitude, ts_str in top5:
                date_str, time_str = ts_str.split()
                yield "Top5_Athens", (date_str, time_str, magnitude)

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       mapper_final=self.mapper_final,
                       reducer=self.reducer)]

if __name__ == "__main__":
    EarthquakeAnalyzer.run()
