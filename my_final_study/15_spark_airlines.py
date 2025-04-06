import matplotlib.pyplot as plt 
from datetime import datetime 
import os 
from pyspark import SparkContext


DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

sc = SparkContext("local","app")


def parse_airlines(line):
    fields = line.split(',')
    try:
        airline_code = int(fields[0])
        airline_symbol = fields[1]
        return (airline_code, airline_symbol)
    except:
        return 


def parse_flights(line):
    fields = line.split(',')

    try: 
        date = datetime.strptime(fields[0], DATE_FMT)
        airline_code = int(fields[1])
        dept = fields[3]
        dest = fields[4]
        dept_delay = int(fields[7])
        arrival_delay = int(fields[10])
        return (date, airline_code, dept, dest, dept_delay, arrival_delay)
    except:
        return 


airline_mapper_rdd = sc.textFile('file:///' + os.getcwd() + '/airlines_no_header.csv') \
                        .map(parse_airlines) \
                        .filter(lambda x: x is not None) \
                        .map(lambda x: (x[0], x[1])) \
                        .collect()  # this rdd acts as a simple mapper between airline codes and symbols - i collect to primary memory into a variable
                        

flights_rdd = sc.textFile('file:///' + os.getcwd() + '/ontime_flights_no_header.csv') \
                .map(parse_flights) \
                .filter(lambda x: x is not None) \
                 .cache()  # i'm caching in primary memory so this rdd persists in the current session and pyspark does not 
                        # perform the same operations multiple times => higher efficiency

## Calculate the sum of dept and arr delays per flight 
question_1 = flights_rdd \
                .map(lambda x: (x[1], x[-2] + x[-1])) \
                .collect()
for delay in question_1[:3]:
    print(f"Flight of {delay[0]} had a total delay of {delay[1]}")