import re
import datetime
from pyspark.sql import Row

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
        month_map[s[3:6]],
        int(s[0:2]),
        int(s[12:14]),
        int(s[15:17]),
        int(s[18:20]))

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (.*?) (\S+)" (\d{3}) (\S+) "(.*?)" "(.*?)"$'

# Returns a dictionary containing the parts of the Apache Access Log.
def parseApacheLogLine(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        # Optionally, you can change this to just ignore if each line of data is not critical.
        #   Corrupt data is common when writing to files.
        #raise Exception("Invalid logline: %s" % logline)
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = int(0)
    else:
        size = int(match.group(9))
    return (Row(
        host    = match.group(1),
        clientIdentd = match.group(2),
        userId       = match.group(3),
        dateTime     = parse_apache_time(match.group(4)),
        method       = match.group(5),
        endpoint     = match.group(6),
        protocol     = match.group(7),
        responseCode = int(match.group(8)),
        contentSize  = size,
        referrer     = match.group(10),
        userAgent    = match.group(11)), 1)

parsed_logs = (spark.sparkContext.textFile("gs://[bucket_name]/[dataset_name]")
    .map(parseApacheLogLine)
    .cache())
parsed_logs.count()

access_logs = (parsed_logs
    .filter(lambda s: s[1] == 1)
    .map(lambda s: s[0])
    .cache())
access_logs.count()

failed_logs = (parsed_logs
    .filter(lambda s: s[1] == 0)
    .map(lambda s: s[0]))
failed_logs_count = failed_logs.count()

if failed_logs_count > 0:
    print("Number of invalid logline: ", failed_logs.count())
    for line in failed_logs.take(20):
        print("Invalid logline: ", line)

print("Read ",parsed_logs.count()," lines, successfully parsed ",access_logs.count()," lines, failed to parse ",failed_logs.count()," lines")

# Calculate statistics based on the content size
content_sizes = access_logs.map(lambda log: log.contentSize).cache()

print ("Content Size Avg: ",content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
       ", Min: ",content_sizes.min(),", Max: ",content_sizes.max())

# Response Code Analysis
responseCodeToCount = (access_logs
    .map(lambda log: (log.responseCode, 1))
    .reduceByKey(lambda a, b : a + b)
    .cache())

responseCodeToCountList = responseCodeToCount.take(100)
    
print("Found ",len(responseCodeToCountList)," response codes")

print("Response Code Counts: ", responseCodeToCount.sortBy(lambda a: -a[1]).take(100))

# Frequent Host Analysis
# Any hosts that has accessed the server more than 10 times
hostCountPairTuple = access_logs.map(lambda log: (log.host, 1))
hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)
hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)
hostsPick20 = (hostMoreThan10
    .map(lambda s: s[0])
    .take(20))
print("Any 20 hosts that have accessed more then 10 times: ", hostsPick20)

# Top Endpoints
endpointCounts = (access_logs
    .map(lambda log: (log.endpoint, 1))
    .reduceByKey(lambda a, b : a + b))
topEndpoints = endpointCounts.takeOrdered(10, lambda s: -1 * s[1])
print("Top Ten Endpoints: ", topEndpoints)

# Top Ten Error Endpoints - What are the top ten endpoints which did not have return code 200
not200 = access_logs.filter(lambda log: log.responseCode != 200)
endpointCountPairTuple = not200.map(lambda log: (log.endpoint, 1))
endpointSum = endpointCountPairTuple.reduceByKey(lambda a, b: a + b)
topTenErrURLs = endpointSum.takeOrdered(10, lambda s: -1 * s[1])
print("Top Ten failed URLs: ", topTenErrURLs)

# Number of Unique Hosts
hosts = access_logs.map(lambda log: log.host)
uniqueHosts = hosts.distinct()
uniqueHostCount = uniqueHosts.count()
print("Unique hosts: ", uniqueHostCount)

# Number of Unique Daily Hosts
dayToHostPairTuple = access_logs.map(lambda log: (log.dateTime.day, log.host))
dayGroupedHosts = dayToHostPairTuple.groupByKey()
dayHostCount = dayGroupedHosts.map(lambda pair: (pair[0], len(set(pair[1]))))
dailyHosts = dayHostCount.sortByKey().cache()
print("Unique hosts per day: ", dailyHosts.sortBy(lambda a: -a[1]).take(30))

# Average Number of Daily Requests per Hosts
dayAndHostTuple = access_logs.map(lambda log: (log.dateTime.day, log.host))
groupedByDay = dayAndHostTuple.groupByKey()
sortedByDay = groupedByDay.sortByKey()
avgDailyReqPerHost = sortedByDay.map(lambda x: (x[0], len(x[1])/len(set(x[1])))).cache()
print("Average number of daily requests per Hosts is ", avgDailyReqPerHost.sortBy(lambda a: -a[1]).take(30))

# How many 404 records are in the log?
badRecords = access_logs.filter(lambda log: log.responseCode==404).cache()
print("Found %d 404 URLs", badRecords.count())

# Listing 404 Response Code Records
badEndpoints = badRecords.map(lambda log: log.endpoint)
badUniqueEndpoints = badEndpoints.distinct()
badUniqueEndpointsPick40 = badUniqueEndpoints.take(40)
print("404 URLS: ", badUniqueEndpointsPick40)

# Listing the Top Twenty 404 Response Code Endpoints**
badEndpointsCountPairTuple = badRecords.map(lambda log: (log.endpoint, 1)).reduceByKey(lambda a, b: a+b)
badEndpointsSum = badEndpointsCountPairTuple.sortBy(lambda x: x[1], False)
badEndpointsTop20 = badEndpointsSum.take(20)
print("Top Twenty 404 URLs: ", badEndpointsTop20)

# Listing the Top Twenty-five 404 Response Code Hosts
errHostsCountPairTuple = badRecords.map(lambda log: (log.host, 1)).reduceByKey(lambda a, b: a+b)
errHostsSum = errHostsCountPairTuple.sortBy(lambda x: x[1], False)
errHostsTop25 = errHostsSum.take(25)
print("Top 25 hosts that generated errors: ", errHostsTop25)

# Listing 404 Response Codes per Day
errDateCountPairTuple = badRecords.map(lambda log: (log.dateTime.day, 1))
errDateSum = errDateCountPairTuple.reduceByKey(lambda a, b: a+b)
errDateSorted = errDateSum.sortByKey().cache()
errByDate = errDateSorted.sortBy(lambda a: -a[1]).collect()
print("404 Errors by day: ", errByDate)

# Visualizing the 404 Response Codes by Day
daysWithErrors404 = errDateSorted.map(lambda x: x[0]).collect()
errors404ByDay = errDateSorted.map(lambda x: x[1]).collect()
# Top Five Days for 404 Response Codes 
topErrDate = errDateSorted.sortBy(lambda x: -x[1]).take(5)
print("Top Five dates for 404 requests: ", topErrDate)

# Hourly 404 Response Codes**
hourCountPairTuple = badRecords.map(lambda log: (log.dateTime.hour, 1))
hourRecordsSum = hourCountPairTuple.reduceByKey(lambda a, b: a+b)
hourRecordsSorted = hourRecordsSum.sortByKey().cache()
errHourList = hourRecordsSorted.sortBy(lambda a: -a[1]).collect()
print("Top hours for 404 requests: ", errHourList)

