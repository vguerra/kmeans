# -*- coding: utf-8 -*-

import os
import random
import sys
from pyspark import SparkContext, SparkConf
from math import sqrt

def computeDistance(x,y):
    return sqrt(sum([(a - b)**2 for a,b in zip(x,y)]))

def hasCentroidConverged(centroideOne, centroideTwo, epsilon):
    return computeDistance(centroideOne, centroideTwo) <= epsilon

def computeClosestClusterInfo(x, centroides):
    # centroides is a dictionary where key is 'centroid Id'
    # and value is coordinates array.
    minDist = sys.float_info.max
    minIndex = -1

    if type(centroides) is dict:
        clusters = [(index, centroid) for (index, centroid) in centroides.items()]
    elif type(centroides) is list:
        clusters = zip(range(len(centroides)), centroides)
    for (index, centroid) in clusters:
        dist = computeDistance(x, centroid)
        if dist < minDist:
            minDist = dist
            minIndex = index

    return (minIndex, minDist)

def sumList(x,y):
    return [x[i] + y[i] for i in range(len(x))]

def moyenneList(x,n):
    return [x[i]/n for i in range(len(x))]

def computeCentroide(stats, default):
    # stats are triplet that contains:
    # counts of points in cluster, sum of all coordinates, total error in cluster
    pointsInCluster = stats[0]
    sumCoordinates = stats[1]
    if pointsInCluster == 0:
        return default
    return [x/pointsInCluster for x in sumCoordinates]

def computeAssignments(points_iterator, centroides):
    pointsProcessed = 0
    pointsPerCluster = {}
    sumsPerCluster = {}
    costPerCluster = {}
    dims = [i for i in range(len(centroides[0]))]

    for centroidId in centroides.keys():
        pointsPerCluster[centroidId] = 0
        sumsPerCluster[centroidId] = [0.0] * len(dims)
        costPerCluster[centroidId] = 0.0

    for point in points_iterator:
        pointsProcessed += 1
        coordinates = point[1][:-1]
        (bestIndex, minDistance) = computeClosestClusterInfo(coordinates, centroides)
        pointsPerCluster[bestIndex] += 1
        costPerCluster[bestIndex] += minDistance 
        for dim in dims:
            sumsPerCluster[bestIndex][dim] += coordinates[dim]

    results = []
    for centroidId in centroides.keys():
        results.append((centroidId, (pointsPerCluster[centroidId], sumsPerCluster[centroidId], costPerCluster[centroidId])))

    return results

#########################
# KMeans implementation #
#########################


##########################
# Initialization methods #
##########################

def initCentroidsRandomly(data, nb_clusters, seed):
    centroides_rdd = sc.parallelize(data.takeSample('withoutReplacment', nb_clusters, seed=seed))\
              .zipWithIndex()\
              .map(lambda x: (x[1], x[0][1][:-1]))
    centroids = centroides_rdd.collectAsMap()
    return centroids

def selectPointsInPartition(partitionIndex, pointsIterator, num_clusters, totalError, iteration, initial_seed):
    if initial_seed is not None:
        random.seed(initial_seed ^ (iteration << 16) ^ partitionIndex)

    filteredPointsWithCost = filter(
        lambda pointWithCost: random.random() < (2.0 * pointWithCost[1] * num_clusters / totalError), pointsIterator)
    filteredPoints = map(
        lambda pointWithCost: pointWithCost[0][1][:-1], filteredPointsWithCost)

    return filteredPoints

def getUniqueCentroides(centroids):
    uniqueCentroids = set(tuple(c) for c in centroids)
    return [list(c) for c in uniqueCentroids]

def chooseCentroidWithCounts(centroids, counts, seed=42):
    if seed is not None:
        random.seed(seed)

    cut = random.random() * sum(counts.values())
    i = 0
    cutCount = 0.0
    while (i < len(centroids) and cutCount < cut):
      cutCount += counts[i]
      i += 1

    return centroids[i - 1]

def initCentroidsParallel(data, nb_iterations, nb_clusters, seed, max_iter=40):
    random.seed(seed)
    firstCentroid = data.takeSample('withoutReplacment', 1, seed=seed)[0][1][:-1]
    dims = range(len(firstCentroid))
    # firstCentroid: [4.9, 2.4, 3.3, 1.0]

    errors = data.map(lambda x: sys.float_info.max)
    iteration = 0

    centroides = [firstCentroid]
    finalCentroids = [firstCentroid]
    bcToDestroy = []

    while iteration < nb_iterations:
        bcCentroides = data.context.broadcast(centroides)
        bcToDestroy.append(bcCentroides)
        prevErrors = errors
        # zipped errors: ((0, [5.1, 3.5, 1.4, 0.2, u'Iris-setosa']), 1.7976931348623157e+308)
        errors = data.zip(prevErrors).map(lambda x: min(computeClosestClusterInfo(x[0][1][:-1], bcCentroides.value)[1], x[1]))
        totalError = errors.sum()

        selectedPoints = data.zip(errors).mapPartitionsWithIndex(
            lambda index, point_iterator: selectPointsInPartition(index, point_iterator, nb_clusters, totalError, iteration, seed)
            ).collect()
        # selectedPoints : [[4.9, 3.0, 1.4, 0.2], [4.9, 3.1, 1.5, 0.1], ...]
        finalCentroids += selectedPoints
        centroides = selectedPoints
        
        iteration += 1

    for bc in bcToDestroy:
        bc.destroy()

    # get unique centroids - we don't really care about ordering
    distinctFinalCentroides = getUniqueCentroides(finalCentroids)
    
    if (len(distinctFinalCentroides) <= nb_clusters):
        return distinctFinalCentroides

    bcFinalCentroids = data.context.broadcast(distinctFinalCentroides)
    countPerCentroid = data.map(lambda x: computeClosestClusterInfo(x[1][:-1], bcFinalCentroids.value)[0]).countByValue()
    bcFinalCentroids.destroy()

    reclusteredCentroids = [chooseCentroidWithCounts(distinctFinalCentroides, countPerCentroid , seed)]
    centroidErrors = list(map(lambda centroid: computeDistance(centroid, reclusteredCentroids[0]), distinctFinalCentroides))

    for i in range(1, nb_clusters):
        scoreSum = sum([count * centroidErrors[idx] for (idx, count) in countPerCentroid.items()])
        cut = random.random() * scoreSum

        j = 0
        cutScore = 0.0
        while (j < len(distinctFinalCentroides) and cutScore < cut):
            cutScore += countPerCentroid[j] * centroidErrors[j]
            j += 1
        if j == 0:
            reclusteredCentroids.append(distinctFinalCentroides[0])
        else:
            reclusteredCentroids.append(distinctFinalCentroides[j - 1])
        
        for idx in countPerCentroid.keys():
            centroidErrors[idx] = min(
                computeDistance(distinctFinalCentroides[idx], reclusteredCentroids[i]),
                centroidErrors[idx]
            )

    # We run a small kmeansto recluster the centroids
    assignedCentroid = [-1] * len(distinctFinalCentroides)
    iter = 0
    clusterinDone = False
    while not clusterinDone and iter < max_iter:
        clusterinDone = True
        counts = [0] * nb_clusters
        sums = [[0.0] * len(dims)] * nb_clusters

        for i in range(len(distinctFinalCentroides)):
            closestCentroidIdx = computeClosestClusterInfo(distinctFinalCentroides[i], reclusteredCentroids)[0]
            for d in dims:
                sums[closestCentroidIdx][d] += countPerCentroid[i] * distinctFinalCentroides[i][d]
            counts[closestCentroidIdx] += countPerCentroid[i]
            if closestCentroidIdx != assignedCentroid[i]:
                clusterinDone = False
                assignedCentroid[i] = closestCentroidIdx
        
        for k in range(nb_clusters):
            if counts[k] == 0:
                # assign random
                reclusteredCentroids[k] = random.choice(distinctFinalCentroides)
            else:
                reclusteredCentroids[k] = moyenneList(sums[k], counts[k])

        iter += 1
    
    centroids = {k : v for (k, v) in zip(range(nb_clusters), reclusteredCentroids)}

    return centroids

def Kmeans(data, nb_clusters, init_mode="parallel", epsilon=1e-4, seed=None):
    clusteringDone = False
    number_of_steps = 0
    current_error = float("inf")

    #############################
    # Select initial centroides #
    #############################

    # data : (0, [5.1, 3.5, 1.4, 0.2, u'Iris-setosa'])]

    if init_mode == "random":
        centroids = initCentroidsRandomly(data, nb_clusters, seed)
    elif init_mode == "parallel":
        centroids = initCentroidsParallel(data, 5, nb_clusters, seed)
    else:
        print("Not a valid initialization mode %s" % init_mode)
        exit()
    # centroids{0: [4.4, 3.0, 1.3, 0.2]}

    print("Step, Error")
    while not clusteringDone:
        bcCentroides = sc.broadcast(centroids)

        #############################
        # Assign points to clusters #
        #############################
        statsPerCluster = data.mapPartitions(lambda points_iter: computeAssignments(points_iter, bcCentroides.value), preservesPartitioning=True)\
                                 .reduceByKey(lambda a, b: (a[0] + b[0], sumList(a[1], b[1]), a[2] + b[2]))\
                                 .collectAsMap()
        # 0: (64, [325.3, 205.10000000000002, 125.2, 28.5], 150.4164215698113)

        ############################################
        # Compute the new centroid of each cluster #
        ############################################
        newCentroides = {centroideId: computeCentroide(stats, centroids[centroideId]) for centroideId, stats in statsPerCluster.items()}

        #################
        # Compute error #
        #################
        current_error = sum([(stat[2]) for stat in statsPerCluster.values()])

        bcCentroides.destroy()
        ############################
        # Is the clustering over ? #
        ############################

        clusteringDone = True
        for (index, newCentroid) in newCentroides.items():
            if (clusteringDone and not hasCentroidConverged(centroids[index], newCentroid, epsilon)):
                clusteringDone = False
            
            centroids[index] = newCentroid

        number_of_steps += 1

        print("%s, %s" % (number_of_steps, current_error))

    assignment = data.map(
        lambda x: (x[0], (computeClosestClusterInfo(x[1][:-1], centroids), x[1]))
    )
    #(0, ((1, 1.3490737563232043), [5.1, 3.5, 1.4, 0.2, u'Iris-setosa']))

    return (assignment, current_error, number_of_steps)


def help():
    print(
    '''Usage of KMeansApp:

    KMeansApp <dataset-path> <num-cluster> <num-features> <init-mode> <implementation-type> [seed]

    dataset-path:        Path to the dataset to be used.
    num-clusters:        Number of clusters to use for the KMeans algorithm
    num-features:        Number of numerical features to be used from the dataset ( not counting the label ).
    init-mode:           Initialization mode to use for choosing initial set of centroids for clustering.
                         Options are: 'random', 'parallel'.
    epsilon              Epsilon to be used for stop condition of KMeans.
    seed:                Optional value to be used as seed for the random choices throughout the algorithm.
    '''
    )


if __name__ == "__main__":

    if (len(sys.argv) < 6 or len(sys.argv) > 7):
        help()
        exit()

    dataset_path = sys.argv[1]
    num_clusters = int(sys.argv[2])
    num_features = int(sys.argv[3])
    init_mode = sys.argv[4]
    epsilon = float(sys.argv[5])
    seed = int(sys.argv[6]) if (len(sys.argv) == 7) else None

    conf = SparkConf()\
        .setAppName("Kmeans ({0}) - Python - dataset: {1}".format(
            init_mode,
            os.path.basename(dataset_path)))\
        .set("spark.driver.host","127.0.0.1")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    lines = sc.textFile(dataset_path, minPartitions=2)

    data = lines.map(lambda x: x.split(','))\
            .map(lambda x: [float(i) for i in x[:num_features]]+[x[num_features]])\
            .zipWithIndex()\
            .map(lambda x: (x[1],x[0]))
    # zipWithIndex allows us to give a specific index to each point
    # (0, [5.1, 3.5, 1.4, 0.2, 'Iris-setosa'])

    clustering = Kmeans(data, nb_clusters=num_clusters, init_mode=init_mode, epsilon=epsilon, seed=None)

    print("Final error: %s achieved on %s steps" % (clustering[1], clustering[2]))
