from pyspark import SparkContext
import matplotlib.pyplot as plt
from datetime import datetime

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"


# Parses a row and returns(airline, dep_delay, arv_delay)
def parse(row):
    try:
        row[0] = datetime.strptime(row[0], DATE_FMT).date()
    except ValueError:
        row[0] = datetime.strptime('1970-01-01', DATE_FMT).date()
    try:
        row[5] = datetime.strptime(row[5], TIME_FMT).time()
    except ValueError:
        row[5] = datetime.strptime('0000', TIME_FMT).time()
    try:
        row[6] = float(row[6])
    except ValueError:
        row[6] = 0.0
    try:
        row[7] = float(row[7])
    except ValueError:
        row[7] = 0.0
    try:
        row[8] = datetime.strptime(row[8], TIME_FMT).time()
    except ValueError:
        row[8] = datetime.strptime('0000', TIME_FMT).time()
    try:
        row[9] = float(row[9])
    except ValueError:
        row[9] = 0.0
    try:
        row[10] = float(row[10])
    except ValueError:
        row[10] = 0.0
    return row[1], row[7], row[10]


sc = SparkContext("local[*]", "Flight Delay Analysis")

# Load the airlines lookup dictionary
airlines = dict(sc.textFile("airlines_no_header.csv")
                .map(lambda line: line.split(','))
                .map(lambda x: (x[0], x[1])).collect())
# Read CSV data
flights = sc.textFile("ontime_flights_no_header.csv")\
               .map(lambda line: line.split(','))\
               .map(parse)
# Calculate sum of departure and arrival delays per flight
delays = flights.map(lambda f: (f[0], f[1] + f[2]))
# Calculate total delays per airline
delays = delays.reduceByKey(lambda x, y: x + y).collect()
delays = sorted(delays, key=lambda x: x[1])

for d in delays:
    print("{0}: {1:10} minutes delayed in total. ({2})".format(d[0], d[1], airlines[d[0]]))

airlines = [d[0] for d in delays]
minutes = [d[1] for d in delays]
plt.barh(airlines, minutes, align='center')
for i, v in enumerate(minutes):
    plt.text(v, i, str(int(v)), color='gray', verticalalignment='center', fontsize=8)
plt.xlabel('Total minutes delayed')
plt.title('Total delays per airline')
plt.savefig('delays.jpg')
# plt.show()
