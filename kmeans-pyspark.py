import os
from math import sqrt
from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans, KMeansModel

conf = SparkConf().setAppName("Kmeans MLLib")
sc = SparkContext(conf=conf)

lines = sc.textFile(os.path.join(".", "input/synthetic-5000-15.data.txt"), minPartitions=2)
data = lines.map(lambda x: x.split(','))\
        .map(lambda x: [float(i) for i in x[:15]])

print(data.take(5))

# Build the model (cluster the data)
clusters = KMeans.train(data, 3, maxIterations=50)

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = data.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))