from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def parseLine(line):
  fields = line.split(',')
  customerId = int(fields[0])
  amount = round(float(fields[2]))
  return (customerId,amount)


lines = sc.textFile("customer-orders.csv")
rdd = lines.map(parseLine)
amountByCustomerId = rdd.reduceByKey(lambda x,y: x+y )
amountByCustomerIdSorted = amountByCustomerId.map(lambda x: (x[1], x[0])).sortByKey()
results = amountByCustomerIdSorted.collect()
for result in results:
  print(result)