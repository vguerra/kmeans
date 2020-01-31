import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.math._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable


object Helpers {

    def computeDistance(x: Array[Float],y: Array[Float]): Float = {
        sqrt((x zip y).map { case (x,y) => pow(y - x, 2) }.sum).toFloat
    }

    def computeClosestClusterInfo(x: Array[Float], centroids: Array[Array[Float]]) : (Long, Float) = {
        // centroids is a dictionary where key is 'centroid Id'
        // and value is coordinates array.
        var minDist = Float.MaxValue
        var minIndex = -1.toLong

        for ((centroid, index) <- centroids.zipWithIndex) {
            val dist = computeDistance(x, centroid)
            if (dist < minDist) {
                minDist = dist
                minIndex = index
            }
        }

        (minIndex, minDist)
    }

    def hasCentroidConverged(centroidOne: Array[Float], centroidTwo: Array[Float], epsilon: Double): Boolean = {
        computeDistance(centroidOne, centroidTwo) <= epsilon
    }

    def chooseCentroidWithCounts(
                                  centroids: Array[Array[Float]],
                                  counts: collection.Map[Long, Long],
                                  seed: Option[Long]) : Array[Float] = {

        val rand = if (seed.isDefined) new scala.util.Random(seed.get) else scala.util.Random
        val cut = rand.nextFloat() * counts.values.sum
        var i = 0
        var cutCount = 0.0
        while (i < centroids.length & cutCount < cut) {
            cutCount += counts.getOrElse[Long](i, 0.toLong)
            i += 1
        }
        centroids(i - 1)
    }

    def selectPointsInPartition(
                                 partitionIndex: Int,
                                 pointsIterator: Iterator[((Long, Array[Float]), Float)],
                                 num_clusters: Int,
                                 totalError: Float,
                                 iteration: Int,
                                 initial_seed: Long): Iterator[Array[Float]] = {

        val rand = new scala.util.Random(initial_seed ^ (iteration << 16) ^ partitionIndex)
        val filteredPoints: Iterator[Array[Float]] = pointsIterator.filter(
            pointWithCost => rand.nextFloat() < (2.0 * pointWithCost._2 * num_clusters / totalError))
          .map(_._1._2)

        filteredPoints
    }

    def sumList(x: Array[Float], y: Array[Float]) : Array[Float] = {
      (x, y).zipped.map(_ + _)
    }

    def moyenneList(x: Array[Float], n: Long) : Array[Float] = {
      x.map(_ / n)
    }

    def computeCentroid(stats: (Long, Array[Float], Float)) : Array[Float] = {
        // # stats are triplet that contains:
        // # counts of points in cluster, sum of all coordinates, total error in cluster
        val pointsInCluster = stats._1
        val sumCoordinates = stats._2
        sumCoordinates.map(_/pointsInCluster.toFloat)
    }


    def computeAssignments(
                            points_iterator: Iterator[(Long, Array[Float])],
                            centroidsDict: Map[Long, Array[Float]]): Iterator[(Long, (Long, Array[Float], Float))] = {

        val dims = (0 to centroidsDict(0).length - 1)
        val centroids = Array.fill[Array[Float]](centroidsDict.size)(Array.fill[Float](dims.length)(0.0.toFloat))
        for ((idx, point) <- centroidsDict) centroids(idx.toInt) = point

        var pointsProcessed = 0
        val pointsPerCluster = Array.fill[Long](centroidsDict.size)(0.toLong)
        val costPerCluster = Array.fill[Float](centroidsDict.size)(0.0.toFloat)
        val sumsPerCluster = Array.fill[Array[Float]](centroidsDict.size)(Array.fill[Float](dims.length)(0.0.toFloat))

        for (point <- points_iterator) {
            pointsProcessed += 1
            val coordinates = point._2
            val (bestIndex, minDistance) = computeClosestClusterInfo(coordinates, centroids)
            val bt = bestIndex.toInt
            pointsPerCluster(bt) += 1
            costPerCluster(bt) += minDistance
            for (dim <- dims)
              sumsPerCluster(bt)(dim) += coordinates(dim)
        }

        val results = (0 to centroids.length - 1)
          .map(idx => (idx.toLong, (pointsPerCluster(idx), sumsPerCluster(idx), costPerCluster(idx))))

        results.toIterator
    }
}

object KMeansRDD {

    def run(dataset: String,
             init_mode: String,
            num_features: Int,
            num_clusters: Int,
            epsilon: Float,
            seed: Option[Long]
           ) : Unit = {

        val conf = new SparkConf()
          .setAppName(s"Kmeans - RDD - ($init_mode) - Scala - dataset: $dataset")
          .set("spark.driver.host","127.0.0.1")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        val lines = sc.textFile(dataset, minPartitions=2)
        val data = lines.map(line => line.split(','))
            .map(point => point.take(num_features).map(_.toFloat))
            .zipWithIndex()
            .map(x => (x._2, x._1))

        val result = kmeans(data, num_clusters, init_mode, epsilon, seed)
        println(s"Final error: ${result._2 } achieved on ${result._3} steps")
    }

    private def kmeans(
                data: RDD[(Long, Array[Float])],
                nb_clusters: Int,
                init_mode:String = "random",
                epsilon: Float,
                seed: Option[Long]
              ) : (RDD[(Long, ((Long, Float), Array[Float]))], Float, Int) = {

        // centroids{0: [4.4, 3.0, 1.3, 0.2]}
        val inital_centroids = init_mode match {
            case "random" => initCentroidsRandomly(data, nb_clusters, seed)
            case "parallel" => initCentroidsParallel(data, nb_clusters, 5, 40, seed)
            case _ => {
                println(s"Init mode '${init_mode}', not allowed. Use 'random' or 'parallel'")
                sys.exit()
            }
        }

        val centroids = mutable.Map[Long, Array[Float]]() ++ inital_centroids

        var clusteringDone = false
        var number_of_steps = 0
        var current_error = Float.MaxValue

        println("Step, Error")
        while (!clusteringDone) {
            val bcCentroides = data.sparkContext.broadcast(centroids.toMap)

            // #############################
            // #Assign points to clusters
            // #############################
            val statsPerCluster = data
              .mapPartitions((points_iter) => Helpers.computeAssignments(points_iter, bcCentroides.value),true)
              .reduceByKey((a, b) => (a._1 + b._1, Helpers.sumList(a._2, b._2), a._3 + b._3))
              .collectAsMap()
            // #0: (64, [325.3, 205.10000000000002, 125.2, 28.5], 150.4164215698113)

            // ############################################
            // #Compute the new centroid of each cluster
            // ############################################

            val newCentroids = statsPerCluster.mapValues(Helpers.computeCentroid)

            // #################
            // #Compute error
            // #################
            current_error = statsPerCluster.mapValues(_._3).values.sum

            bcCentroides.destroy()

            // ############################
            // #Is the clustering over ?
            // ############################

            clusteringDone = true
            for ((index, newCentroid) <- newCentroids) {
                if (clusteringDone && !Helpers.hasCentroidConverged(centroids(index), newCentroid, epsilon))
                    clusteringDone = false

                centroids(index) = newCentroid
            }
            number_of_steps += 1

            println(s"${number_of_steps}, ${current_error}")
        }

        val finalCentroids = centroids.toSeq.sortBy(_._1).map(_._2).toArray
        val assignment = data.map(
            x => (x._1, (Helpers.computeClosestClusterInfo(x._2, finalCentroids), x._2))
        )
        // #(0, ((1, 1.3490737563232043), [5.1, 3.5, 1.4, 0.2]))

        (assignment, current_error, number_of_steps)
    }


    // Centroids initialization section


    private def initCentroidsRandomly(
                                       data: RDD[(Long, Array[Float])],
                                       nb_cluster: Int,
                                       seed: Option[Long]
                                     ) : Map[Long, Array[Float]] = {

        val samples: Array[(Long, Array[Float])] = seed match {
            case Some(seedValue) => data.takeSample(false, nb_cluster, seedValue)
            case None => data.takeSample(false, nb_cluster)
        }

        val centroids = samples.map(_._2).zipWithIndex.map(c => (c._2.toLong, c._1)).toMap

        centroids
    }

    private def initCentroidsParallel(
                                       data: RDD[(Long, Array[Float])],
                                       nb_clusters: Int,
                                       nb_iterations: Int,
                                       max_iter: Int=40,
                                       seed: Option[Long]
                                     ) : Map[Long, Array[Float]] = {

        val oneSample: Array[(Long, Array[Float])] = seed match {
            case Some(seedValue) => data.takeSample(false, 1, seedValue)
            case None => data.takeSample(false, 1)
        }

        val firstCentroid = oneSample(0)._2
        val dims = firstCentroid.length

        var errors = data.map(_ => Float.MaxValue)

        val finalCentroids = ArrayBuffer[Array[Float]]()
        var centroids = Array(firstCentroid)
        val bcToDestroy = ArrayBuffer[Broadcast[_]]()

        var iteration = 0
        var prevErrors = errors

        while (iteration < nb_iterations) {
            val bcCentroids = data.context.broadcast(centroids)
            bcToDestroy.append(bcCentroids)
            prevErrors = errors

            // zipped errors: ((0, [5.1, 3.5, 1.4, 0.2]), 1.7976931348623157e+308)
            errors = data
              .zip(prevErrors)
              .map(x => min(Helpers.computeClosestClusterInfo(x._1._2, bcCentroids.value)._2, x._2))

            val totalError = errors.sum().toFloat

            val selectedPoints: Array[Array[Float]] = data
              .zip(errors)
              .mapPartitionsWithIndex(
                  (index, point_iterator) =>  Helpers.selectPointsInPartition(
                      index,
                      point_iterator,
                      nb_clusters,
                      totalError,
                      iteration,
                      seed.getOrElse(0)))
              .collect()
            // selectedPoints:[[4.9, 3.0, 1.4, 0.2], [4.9, 3.1, 1.5, 0.1],...]

            finalCentroids.appendAll(selectedPoints)
            centroids = selectedPoints

            iteration += 1
        }

        bcToDestroy.foreach(_.destroy())

        // get unique centroids - we don't really care about ordering
        var distinctFinalCentroids = finalCentroids.distinct.toArray

        if (distinctFinalCentroids.length <= nb_clusters) {
            return distinctFinalCentroids.zipWithIndex.map(c => (c._2.toLong, c._1)).toMap
        }

        val bcFinalCentroids = data.context.broadcast(distinctFinalCentroids)
        val countPerCentroid = data.map(
            x => Helpers.computeClosestClusterInfo(x._2, bcFinalCentroids.value)._1).countByValue()
        bcFinalCentroids.destroy()

        val reclusteredCentroids = ArrayBuffer[Array[Float]](Helpers.chooseCentroidWithCounts(
            distinctFinalCentroids, countPerCentroid , seed))

        val centroidErrors: Array[Float] = distinctFinalCentroids.map(
            centroid => Helpers.computeDistance(centroid, reclusteredCentroids(0)))

        val rand = if (seed.isDefined) new scala.util.Random(seed.get) else scala.util.Random

        for (i <- 1 to nb_clusters - 1) {
            var scoreSum: Double = 0.0
            for ((idx, count) <- countPerCentroid) {
                scoreSum += count * centroidErrors(idx.toInt)
            }

            val cut = rand.nextFloat() * scoreSum
            var j = 0
            var cutScore = 0.0
            while (j < distinctFinalCentroids.length && cutScore < cut) {
                cutScore += countPerCentroid(j) * centroidErrors(j)
                j += 1
            }
            if (j == 0)
                reclusteredCentroids.append(distinctFinalCentroids(0))
            else
                reclusteredCentroids.append(distinctFinalCentroids(j - 1))

            for (k <- countPerCentroid.keys) {
                val key = k.toInt
                centroidErrors(key) = min(
                    Helpers.computeDistance(distinctFinalCentroids(key), reclusteredCentroids(i)),
                    centroidErrors(key)
                )
            }
        }

        // We run a small kmeans to re-cluster the centroids
        val assignedCentroid = Array.fill[Long](distinctFinalCentroids.length)(-1)
        var iter = 0
        var clusterinDone = false
        while (!clusterinDone &&  iter < max_iter) {
            clusterinDone = true
            val counts = Array.fill[Long](nb_clusters)(0)
            val sums = Array.fill[Array[Float]](nb_clusters)(Array.fill[Float](dims)(0.0.toFloat))

            for (i <- 0 to distinctFinalCentroids.length - 1) {
                val closestCentroidIdx = Helpers.computeClosestClusterInfo(distinctFinalCentroids(i), reclusteredCentroids.toArray)._1.toInt
                for (d <- 0 to  dims - 1) {
                  sums(closestCentroidIdx)(d) += countPerCentroid(i) * distinctFinalCentroids(i)(d)
                }
                counts(closestCentroidIdx) += countPerCentroid(i)

                if (closestCentroidIdx != assignedCentroid(i)) {
                    clusterinDone = false
                    assignedCentroid(i) = closestCentroidIdx
                }
            }

            for (k <- 0 to nb_clusters - 1) {
                if (counts(k) == 0)
                    reclusteredCentroids(k) = distinctFinalCentroids(Random.nextInt(distinctFinalCentroids.size))
                else
                    reclusteredCentroids(k) = Helpers.moyenneList(sums(k), counts(k))
            }

            iter += 1
        }


        val chosenCentroids = (0 to reclusteredCentroids.length - 1).map(_.toLong).zip(reclusteredCentroids).toMap

        chosenCentroids
    }
}