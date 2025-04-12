from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import time 
from datetime import datetime 
import numpy as np

cols = 'ticker,date,open,high,low,close'.split(',')

class BtcAnalyzer(MRJob):
    linec = 0

    def mapper(self, _, line):
        BtcAnalyzer.linec += 1          
        # ingesting the line data - making sure to ignore the header line
        if BtcAnalyzer.linec == 1:
            return 

        # ingesting non-header data
        fields = line.split(',')
        try:
            ticker = fields[0]
            date = datetime.strptime(fields[1], "%Y-%m-%d")
            open = float(fields[2])
            high = float(fields[3])
            low = float(fields[4])
            close = float(fields[5])
        except:
            return  # ignoring invalid lines...
        
        if date.year == 2020:
            yield "BTC_Closing_2020", close  # returns ("BTC_Closing_2020", float) key-value pairs to facilitate analysis for Q1; the float values will be averaged
        
        if close > 60_000:
            yield "Over_60K", 1  # returns ("Over_60K", 1) key-value pairs to facilitate Q2; the [1,1,1,1, ...] array will just be summed to get the corresponding result

        if close < (high + low) / 2:
            yield "Lower_Avg_of_HL", 1  # facilitates Q3 the same way as we treat Q2

        yield "Daily_Close", (close, date.strftime("%Y-%m-%d"))   # for Q4 - i will sort in descending order and print out the dates and prices

        yield f"Q5_{date.year}_{date.month}", close  # for Q5 - i will calc the mean close per month and only keep those who have avg close in [20_000, 25_000]

    
    # Each mapper will automatically be assigned the handling of a particular set of lines. 
    # Now the reducer will get 4 + (as many distinct months are in the 'date' column of the csv) key-value pairs
    # For Q1 -> ("BTC_Closing_2020", [daily_close1, daily_close2, ...]) => AVERAGE
    # For Q2 -> ("Over_60K", [1,1,1,1,1, ...]) => SUM
    # For Q3 -> ("Lower_Avg_of_HL", [1,1,1,1, ...]) => SUM
    # For Q4 -> ("Daily_Close", [(close1, date1), (close2, date2), ...]) => SORT in descending order and only keep the first 10
    # For Q5 -> ((year1, month1), [close11_1, close11_2, ...]), ((year2, month2), [close12_1, close12_2, ...]) => avg each and filter and show in ascending order
    def reducer(self, key, values):
        if key == "BTC_Closing_2020":
            values = list(values)
            yield "Q1_Avg_Closing_Price_BTC_2020", float(np.mean(values))  # numpy values are not json subscriptable

        elif key == "Over_60K":
            values = list(values)
            yield "Q2_Count_Dates_Closing_Over_60K", float(np.sum(values))

        elif key == "Lower_Avg_of_HL":
            values = list(values)
            yield "Q3_Count_Days_Closing_Less_Avg_High_Low", float(np.sum(values))

        elif key == "Daily_Close":
            values = list(values)
            values = sorted(values, reverse=True)[:10]
            for value in values:
                yield "Q4_10_Highest_Closing_w_Dates", f'Date: {value[1]}, Closing: {value[0]}'

        elif key.startswith("Q5_"):
            values = list(values)
            mean_val = np.mean(values)
            if mean_val >= 20_000 and mean_val <= 22_000:
                yield "Q5_Month_w_Closing_Between_20K_22K", key



if __name__ == "__main__":
    BtcAnalyzer.run()