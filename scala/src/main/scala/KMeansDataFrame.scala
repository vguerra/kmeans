import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{DataType, FloatType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

import scala.collection.mutable

// Based on example in https://docs.databricks.com/spark/latest/spark-sql/udaf-scala.html
class SumCoordinates(dim: Int) extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: org.apache.spark.sql.types.StructType =
        StructType(StructField("coordinates", ArrayType(FloatType)) :: Nil)

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType = StructType(
        StructField("sumOfCoordinates", ArrayType(FloatType)) :: Nil
    )

    // This is the output type of your aggregatation function.
    override def dataType: DataType = ArrayType(FloatType)

    override def deterministic: Boolean = true

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = Array.fill[Float](dim)(0.0.toFloat)
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = Helpers.sumList(
            buffer.getAs[Seq[Float]](0).toArray,
            input.getAs[Seq[Float]](0).toArray
        )
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = Helpers.sumList(
            buffer1.getAs[Seq[Float]](0).toArray,
            buffer2.getAs[Seq[Float]](0).toArray
        )
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
        buffer.getAs[Seq[Float]](0).toArray
    }
}


object HelpersDF {

    def computeAssignments(
                            points_iterator: Iterator[Row],
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
            val coordinates = point.getAs[Seq[Float]]("features").toArray
            val (bestIndex, minDistance) = Helpers.computeClosestClusterInfo(coordinates, centroids)
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

    // got from https://stackoverflow.com/a/32634826
    def castColumnTo( df: DataFrame, columnName: String, toType: DataType ) : DataFrame = {
        df.withColumn( columnName, df(columnName).cast(toType))
    }
}

object KMeansDataFrame {
    def run(dataset_path: String,
            init_mode: String,
            num_features: Int,
            num_clusters: Int,
            epsilon: Float,
            seed: Option[Long]
           ) : Unit = {

        val spark = SparkSession
          .builder()
          .appName(s"Kmeans - DataFrame - ($init_mode) - Scala - dataset: ${dataset_path}")
          .config("spark.driver.host","127.0.0.1")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        var df = spark.read.csv(dataset_path)
        df = df.drop(s"_c${num_features}")

        (0 to num_features - 1).foreach(idx => df = HelpersDF.castColumnTo(df, s"_c${idx}", FloatType))

        val cols = (0 to num_features - 1).map(idx => col(s"_c${idx}"))
        df = df.withColumn("features", array(cols:_*))

        val result = kmeans(df, num_clusters, init_mode, num_features, epsilon, None)
        println(s"Final error: ${result._2 } achieved on ${result._3} steps")
    }

    private def kmeans(
                data: DataFrame,
                nb_clusters: Int,
                init_mode:String = "random",
                num_features: Int,
                epsilon: Float,
                seed: Option[Long]
              ) : (DataFrame, Double, Int) = {

        // centroids{0: [4.4, 3.0, 1.3, 0.2]}
        val inital_centroids: Map[Long, Array[Float]] = init_mode match {
            case "random" => initCentroidsRandomly(data, nb_clusters, seed)
            case "parallel" => initCentroidsParallel(data, nb_clusters, 5, num_features, 40, seed)
            case _ => {
                println(s"Init mode '${init_mode}', not allowed. Use 'random' or 'parallel'")
                sys.exit()
            }
        }

        val centroids = mutable.Map[Long, Array[Float]]() ++ inital_centroids

        var clusteringDone = false
        var number_of_steps = 0
        var current_error = Float.MaxValue

        import data.sparkSession.sqlContext.implicits._

        println("Step, Error")
        while (!clusteringDone) {
            val bcCentroides = data.sparkSession.sparkContext.broadcast(centroids.toMap)

            // #############################
            // #Assign points to clusters
            // #############################
            val sumCoordinatesUDF = new SumCoordinates(num_features)
            val statsPerCluster = data
              .mapPartitions(
                  points_iter => HelpersDF.computeAssignments(
                      points_iter,
                      bcCentroides.value))
              .groupBy("_1")
              .agg(
                  sum($"_2".getField("_1")).alias("pointsPerCluster"),
                  sumCoordinatesUDF($"_2".getField("_2")).alias("sumCoordinates"),
                  sum($"_2".getField("_3")).alias("totalError")
              )
              .collect()
              .map(row => (
                row.getAs[Long](0),
                (row.getAs[Long](1), row.getAs[Seq[Float]](2).toArray, row.getAs[Double](3).toFloat)))
              .toMap
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
            x => (x.getAs[Array[Float]](0), Helpers.computeClosestClusterInfo(x.getAs[Array[Float]](0), finalCentroids))
        ).toDF("features", "assignmentInfo")
        // #(0, ((1, 1.3490737563232043), [5.1, 3.5, 1.4, 0.2]))

        (assignment, current_error, number_of_steps)
    }


    // Centroids initialization section


    private def initCentroidsRandomly(
                                       data: DataFrame,
                                       nb_cluster: Int,
                                       seed: Option[Long]
                                     ) : Map[Long, Array[Float]] = {

        val ratio = nb_cluster.toDouble / data.count() + 0.05 // To make sure we get enough points
        val samples: Dataset[Row] = seed match {
            case Some(seedValue) => data.sample(false, ratio, seedValue)
            case None => data.sample(false, ratio)
        }

        val centroids = samples
          .collect()
          .take(nb_cluster)
          .zipWithIndex
          .map( c => (c._2.toLong, c._1.getAs[Seq[Float]]("features").toArray))
          .toMap

        centroids
    }

    private def initCentroidsParallel(
                                       data: DataFrame,
                                       nb_clusters: Int,
                                       nb_iterations: Int,
                                       num_features: Int,
                                       max_iter: Int=40,
                                       seed: Option[Long]
                                     ) : Map[Long, Array[Float]] = {

        val ratio = 4.toFloat / data.count()

        val oneSample: Dataset[Row] = seed match {
            case Some(seedValue) => data.sample(false, ratio, seedValue)
            case None => data.sample(false, ratio)
        }

        val tmp: Row = oneSample.take(1)(0)
        val mp: Map[Long, Array[Float]] = Map(
            0.toLong -> tmp.getAs[Seq[Float]]("features").toArray
        )

        val firstCentroid  = tmp
        val dims = num_features

        var dataWithErrors = data.withColumn("prev_errors", lit(Float.MaxValue))
        dataWithErrors = dataWithErrors.withColumn("errors", lit(Float.MaxValue))

        val finalCentroids = ArrayBuffer[Array[Float]]()
        var centroids = Array(firstCentroid.getAs[Seq[Float]]("features").toArray)
        val bcToDestroy = ArrayBuffer[Broadcast[_]]()

        var iteration = 0
        while (iteration < nb_iterations) {

            val bcCentroids = data.sparkSession.sparkContext.broadcast(centroids)
            bcToDestroy.append(bcCentroids)

            dataWithErrors = dataWithErrors
              .withColumn("prev_errors", col("errors"))
            // zipped errors: ((0, [5.1, 3.5, 1.4, 0.2]), 1.7976931348623157e+308)

            val minErrorFun: (Seq[Float], Float) => Float = (coordinates: Seq[Float], prev_error: Float) => {
                scala.math.min(Helpers.computeClosestClusterInfo(coordinates.toArray, bcCentroids.value)._2,
                    prev_error
                )
            }

            val minErrorSqlFun = udf(minErrorFun)

            dataWithErrors = dataWithErrors
              .withColumn("errors", minErrorSqlFun(col("features"), col("prev_errors")))

            val totalError = dataWithErrors.agg(sum("errors")).first().getAs[Double](0).toFloat

            val rand = new scala.util.Random(seed.getOrElse[Long](0) ^ (iteration << 16))
            val distFilterFun: (Float) => Boolean = (error: Float) => {
                rand.nextFloat < (2.0 * error * nb_clusters / totalError)
            }

            val distFilterSqlFun = udf(distFilterFun)

            val selectedPoints: Array[Array[Float]] = dataWithErrors
              .filter(distFilterSqlFun(col("errors")))
              .select("features")
              .collect()
              .map(row => row.getAs[Seq[Float]]("features").toArray)
            // selectedPoints:[[4.9, 3.0, 1.4, 0.2], [4.9, 3.1, 1.5, 0.1],...]

            finalCentroids.appendAll(selectedPoints)
            centroids = selectedPoints

            iteration += 1
        }

        bcToDestroy.foreach(_.destroy())

        // get unique centroids - we don't really care about ordering
        var distinctFinalCentroids: Array[Array[Float]] = finalCentroids.distinct.toArray

        if (distinctFinalCentroids.length <= nb_clusters) {
            return distinctFinalCentroids.zipWithIndex.map(c => (c._2.toLong, c._1)).toMap
        }

        val bcFinalCentroids = data.sparkSession.sparkContext.broadcast(distinctFinalCentroids)

        val assignedClusterFun: (Seq[Float]) => Long = (coordinates: Seq[Float]) => {
            Helpers.computeClosestClusterInfo(coordinates.toArray, bcFinalCentroids.value)._1
        }
        val assignedClusterSqlFun = udf(assignedClusterFun)

        val countPerCentroid = data
          .withColumn("assignedCluster", assignedClusterSqlFun(col("features")))
          .groupBy(col("assignedCluster"))
          .count()
          .collect()
          .map(
              row => (
                row.getAs[Long]("assignedCluster"),
                row.getAs[Long]("count")
              ))
          .toMap

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
                cutScore += countPerCentroid.getOrElse(j, 0.toLong) * centroidErrors(j)
                j += 1
            }
            if (j == 0)
                reclusteredCentroids.append(distinctFinalCentroids(0))
            else
                reclusteredCentroids.append(distinctFinalCentroids(j - 1))

            for (k <- countPerCentroid.keys) {
                val key = k.toInt
                centroidErrors(key) = scala.math.min(
                    Helpers.computeDistance(distinctFinalCentroids(key), reclusteredCentroids(i)),
                    centroidErrors(key)
                )
            }
        }

        // We run a small kmeansto recluster the centroids
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
                  sums(closestCentroidIdx)(d) += countPerCentroid.getOrElse(i, 0.toLong) * distinctFinalCentroids(i)(d)
                }
                counts(closestCentroidIdx) += countPerCentroid.getOrElse(i, 0.toLong)

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