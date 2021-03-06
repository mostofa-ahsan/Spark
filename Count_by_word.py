from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def norm_word(line):
	return re.compile(r'\W+',re.UNICODE).split(line.lower())

input = sc.textFile("file:///sparkcourse/book.txt")

words = input.flatMap(norm_word)
count_words = words.map(lambda x: (x, 1)).reduceByKey(lambda x,y : x+y)
sorted_count_words = count_words.map(lambda x: (x[1],x[0])).sortByKey()
# count_words = words.countByValue()

results = sorted_count_words.collect()

for k, v in results:
	cor_word = v.encode('ascii','ignore')
	if cor_word:
		print(f" WORD -- {cor_word} -- appears--- {k}")