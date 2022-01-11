from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("MaxTemperature")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationId = fields[0]
    entryType = fields[2]
    maxTemp = float(fields[3])
    return (stationId, entryType, maxTemp)

lines = sc.textFile("/Users/aranibasu/Desktop/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
maxTemps = parsedLines.filter(lambda x : "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x : (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y : min(x, y))
results = maxTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
    
    
