from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MostViewedMovies")
sc = SparkContext(conf = conf)

def loadMovies():
	movieNames={}
	with open("ml-100k/u.ITEM") as f:
		for line in f:
			field = line.split('|')
			movieNames[int(field[0])]=field[1]
	return movieNames


nameDict = sc.broadcast(loadMovies())

lines= sc.textFile("file:///SParkCourse/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]),1))
MovieCounts = movies.reduceByKey(lambda x,y : x+y)

Counts = MovieCounts.map(lambda x: (x[1],x[0]))
sorted_counts = Counts.sortByKey()

sortedWithNames = sorted_counts.map(lambda countMovie:(nameDict.value[countMovie[1]],countMovie[0]))

results = sortedWithNames.collect()

for i in results:
	print(i)

