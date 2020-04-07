from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustOrder")
sc = SparkContext(conf = conf)


input = sc.textFile("file:///sparkcourse/customer-orders.csv")
def parseLines(line):
	field = line.split(',')
	c_id = field[0]
	order_amt= field[2]
	return(int(c_id),float(order_amt))

rdd= input.map(parseLines)
Total_amt = rdd.reduceByKey(lambda x,y: x+y)

results = Total_amt.collect()

for k,v in results:
	print(f"Customer ID {k} has total {v} order")




