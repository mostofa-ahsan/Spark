from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def norm_word(line):
	return re.compile(r'\W+',re.UNICODE).split(line.lower())

input = sc.textFile("file:///sparkcourse/book.txt")

words = input.flatMap(norm_word)
count_words = words.countByValue()

# results = count_words.collect()

for k, v in count_words.items():
	cor_word = k.encode('ascii','ignore')
	if cor_word:
		print(f" WORD -- {cor_word} -- appears--- {v}")