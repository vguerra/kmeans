
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{FloatType, StructField, StructType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object HelpersDS {
  def computeAssignments[R <: KMeansDataSet.CustomRow](
                                                        points_iterator: Iterator[R],
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
      val coordinates = point.toArray()
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
}

object KMeansDataSet {

  object Schemas {
    val Iris = {
      StructType(Array(
        StructField("_c0", FloatType, true),
        StructField("_c1", FloatType, true),
        StructField("_c2", FloatType, true),
        StructField("_c3", FloatType, true),
        StructField("_c4", StringType, true)
      ))
    }
    val Spam = {
      StructType(Array(
        StructField("_c0", FloatType, true),
        StructField("_c1", FloatType, true),
        StructField("_c2", FloatType, true),
        StructField("_c3", FloatType, true),
        StructField("_c4", FloatType, true),
        StructField("_c5", FloatType, true),
        StructField("_c6", FloatType, true),
        StructField("_c7", FloatType, true),
        StructField("_c8", FloatType, true),
        StructField("_c9", FloatType, true),
        StructField("_c10", FloatType, true),
        StructField("_c11", FloatType, true),
        StructField("_c12", FloatType, true),
        StructField("_c13", FloatType, true),
        StructField("_c14", FloatType, true),
        StructField("_c15", FloatType, true),
        StructField("_c16", FloatType, true),
        StructField("_c17", FloatType, true),
        StructField("_c18", FloatType, true),
        StructField("_c19", FloatType, true),
        StructField("_c20", FloatType, true),
        StructField("_c21", FloatType, true),
        StructField("_c22", FloatType, true),
        StructField("_c23", FloatType, true),
        StructField("_c24", FloatType, true),
        StructField("_c25", FloatType, true),
        StructField("_c26", FloatType, true),
        StructField("_c27", FloatType, true),
        StructField("_c28", FloatType, true),
        StructField("_c29", FloatType, true),
        StructField("_c30", FloatType, true),
        StructField("_c31", FloatType, true),
        StructField("_c32", FloatType, true),
        StructField("_c33", FloatType, true),
        StructField("_c34", FloatType, true),
        StructField("_c35", FloatType, true),
        StructField("_c36", FloatType, true),
        StructField("_c37", FloatType, true),
        StructField("_c38", FloatType, true),
        StructField("_c39", FloatType, true),
        StructField("_c40", FloatType, true),
        StructField("_c41", FloatType, true),
        StructField("_c42", FloatType, true),
        StructField("_c43", FloatType, true),
        StructField("_c44", FloatType, true),
        StructField("_c45", FloatType, true),
        StructField("_c46", FloatType, true),
        StructField("_c47", FloatType, true),
        StructField("_c48", FloatType, true),
        StructField("_c49", FloatType, true),
        StructField("_c50", FloatType, true),
        StructField("_c51", FloatType, true),
        StructField("_c52", FloatType, true),
        StructField("_c53", FloatType, true),
        StructField("_c54", FloatType, true),
        StructField("_c55", FloatType, true),
        StructField("_c56", FloatType, true),
        StructField("_c57", FloatType, true)
      ))
    }
    val Syn5000 = {
      StructType(Array(
        StructField("_c0", FloatType, true),
        StructField("_c1", FloatType, true),
        StructField("_c2", FloatType, true),
        StructField("_c3", FloatType, true),
        StructField("_c4", FloatType, true),
        StructField("_c5", FloatType, true),
        StructField("_c6", FloatType, true),
        StructField("_c7", FloatType, true),
        StructField("_c8", FloatType, true),
        StructField("_c9", FloatType, true),
        StructField("_c10", FloatType, true),
        StructField("_c11", FloatType, true),
        StructField("_c12", FloatType, true),
        StructField("_c13", FloatType, true),
        StructField("_c14", FloatType, true),
        StructField("_c15", FloatType, true)
      ))
    }
  }

  abstract class CustomRow {
    def toArray(): Array[Float]

    def dim: Long
  }

  case class IrisRow(_c0: Float, _c1: Float, _c2: Float, _c3: Float, _c4: String) extends CustomRow {
    override def toArray(): Array[Float] = {
      Array(_c0, _c1, _c2, _c3)
    }

    override def dim: Long = 4
  }

  case class SpamRow(
                      _c0: Float, _c1: Float, _c2: Float, _c3: Float, _c4: Float, _c5: Float, _c6: Float, _c7: Float, _c8: Float, _c9: Float,
                      _c10: Float, _c11: Float, _c12: Float, _c13: Float, _c14: Float, _c15: Float, _c16: Float, _c17: Float, _c18: Float, _c19: Float,
                      _c20: Float, _c21: Float, _c22: Float, _c23: Float, _c24: Float, _c25: Float, _c26: Float, _c27: Float, _c28: Float, _c29: Float,
                      _c30: Float, _c31: Float, _c32: Float, _c33: Float, _c34: Float, _c35: Float, _c36: Float, _c37: Float, _c38: Float, _c39: Float,
                      _c40: Float, _c41: Float, _c42: Float, _c43: Float, _c44: Float, _c45: Float, _c46: Float, _c47: Float, _c48: Float, _c49: Float,
                      _c50: Float, _c51: Float, _c52: Float, _c53: Float, _c54: Float, _c55: Float, _c56: Float, _c57: Float
                    ) extends CustomRow {
    override def toArray(): Array[Float] = {
      Array(
        _c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9,
        _c10, _c11, _c12, _c13, _c14, _c15, _c16, _c17, _c18, _c19,
        _c20, _c21, _c12, _c23, _c24, _c25, _c26, _c27, _c28, _c29,
        _c30, _c31, _c32, _c33, _c34, _c35, _c36, _c37, _c38, _c39,
        _c40, _c41, _c42, _c43, _c44, _c45, _c46, _c47, _c48, _c49,
        _c50, _c51, _c52, _c53, _c54, _c55, _c56
      )
    }

    override def dim: Long = 57
  }

  case class Syn5000Row(
                         _c0: Float, _c1: Float, _c2: Float, _c3: Float, _c4: Float, _c5: Float, _c6: Float, _c7: Float, _c8: Float, _c9: Float,
                         _c10: Float, _c11: Float, _c12: Float, _c13: Float, _c14: Float, _c15: Float
                       ) extends CustomRow {
    override def toArray(): Array[Float] = {
      Array(
        _c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9,
        _c10, _c11, _c12, _c13, _c14
      )
    }

    override def dim: Long = 15
  }


  def run(dataset_path: String,
          init_mode: String,
          num_features: Int,
          num_clusters: Int,
          epsilon: Float,
          seed: Option[Long]
         ): Unit = {

    val spark = SparkSession
      .builder()
      .appName(s"Kmeans - DataFrame - ($init_mode) - Scala - dataset: $dataset_path")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val ds = if (dataset_path.contains("iris")) spark.read.option("inferSchema", "true").schema(Schemas.Iris).csv(dataset_path).as[IrisRow]
    else if (dataset_path.contains("spam")) spark.read.option("inferSchema", "true").schema(Schemas.Spam).csv(dataset_path).as[SpamRow]
    else spark.read.option("inferSchema", "true").schema(Schemas.Syn5000).csv(dataset_path).as[Syn5000Row]

    val result = kmeans(ds, num_clusters, init_mode, num_features, epsilon, None)
    println(s"Final error: ${result._2} achieved on ${result._3} steps")
  }

  private def kmeans[R <: CustomRow](
                                      data: Dataset[R],
                                      nb_clusters: Int,
                                      init_mode: String = "random",
                                      num_features: Int,
                                      epsilon: Float,
                                      seed: Option[Long]
                                    ): (Dataset[(Array[Float], (Long, Float))], Double, Int) = {

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
          points_iter => HelpersDS.computeAssignments(
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
    val assignment: Dataset[(Array[Float], (Long, Float))] = data.map(
      x => (x.toArray(), Helpers.computeClosestClusterInfo(x.toArray(), finalCentroids))
    )
    // #(0, ((1, 1.3490737563232043), [5.1, 3.5, 1.4, 0.2]))

    (assignment, current_error, number_of_steps)
  }


  // Centroids initialization section


  private def initCentroidsRandomly[R <: CustomRow](
                                                     data: Dataset[R],
                                                     nb_cluster: Int,
                                                     seed: Option[Long]
                                                   ): Map[Long, Array[Float]] = {

    val ratio = nb_cluster.toDouble / data.count() + 0.05 // To make sure we get enough points
    val samples = seed match {
      case Some(seedValue) => data.sample(false, ratio, seedValue)
      case None => data.sample(false, ratio)
    }

    val centroids = samples
      .collect()
      .take(nb_cluster)
      .zipWithIndex
      .map(c => (c._2.toLong, c._1.toArray()))
      .toMap

    centroids
  }

  private def initCentroidsParallel[R <: CustomRow](
                                                     data: Dataset[R],
                                                     nb_clusters: Int,
                                                     nb_iterations: Int,
                                                     num_features: Int,
                                                     max_iter: Int = 40,
                                                     seed: Option[Long]
                                                   ): Map[Long, Array[Float]] = {

    val oneSample = seed match {
      case Some(seedValue) => data.sample(false, 0.02, seedValue)
      case None => data.sample(false, 0.02)
    }

    val tmp: CustomRow = oneSample.take(1)(0)
    val mp: Map[Long, Array[Float]] = Map(
      0.toLong -> tmp.toArray()
    )

    val firstCentroid = tmp
    val dims = num_features

    var dataWithErrors: DataFrame = data.withColumn("prev_errors", lit(Float.MaxValue))
    dataWithErrors = dataWithErrors.withColumn("errors", lit(Float.MaxValue))
    val cols = (0 to num_features - 1).map(idx => col(s"_c${idx}"))
    dataWithErrors = dataWithErrors.withColumn("features", array(cols: _*))

    val finalCentroids = ArrayBuffer[Array[Float]]()
    var centroids = Array(firstCentroid.toArray())
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

    val countPerCentroid = dataWithErrors
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

    val reclusteredCentroids = ArrayBuffer[Array[Float]](Helpers.chooseCentroidWithCounts(
      distinctFinalCentroids, countPerCentroid, seed))

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
    while (!clusterinDone && iter < max_iter) {
      clusterinDone = true
      val counts = Array.fill[Long](nb_clusters)(0)
      val sums = Array.fill[Array[Float]](nb_clusters)(Array.fill[Float](dims)(0.0.toFloat))

      for (i <- 0 to distinctFinalCentroids.length - 1) {
        val closestCentroidIdx = Helpers.computeClosestClusterInfo(distinctFinalCentroids(i), reclusteredCentroids.toArray)._1.toInt
        for (d <- 0 to dims - 1) {
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