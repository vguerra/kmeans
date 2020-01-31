import java.lang.System.err

object KMeansApp {

  def help(): Unit = {
    err.println(
      """Usage of KMeansApp:
        |
        |KMeansApp <dataset-path> <num-cluster> <num-features> <init-mode> <implementation-type> [seed]
        |
        |dataset-path:        Path to the dataset to be used.
        |num-clusters:        Number of clusters to use for the KMeans algorithm
        |num-features:        Number of numerical features to be used from the dataset ( not counting the label ).
        |init-mode:           Initialization mode to use for choosing initial set of centroids for clustering.
        |                     Options are: 'random', 'parallel'.
        |implementation-type: The type of implementation to be used.
        |                     Options are: 'rdd', 'dataset', 'dataframe'.
        |epsilon:             Epsilon to be used in stop condition of KMeans algorithm.
        |seed:                Optional value to be used as seed for the random choices throughout the algorithm.
        |""".stripMargin)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 6 || args.length > 7) {
      help()
      return
    }

    val dataset_path = args(0)
    val num_clusters = args(1).toInt
    val num_features = args(2).toInt
    val init_mode = args(3)
    val implementation_type = args(4)
    val epsilon = (5).toFloat
    val seed:Option[Long] = if (args.length == 7) Some(args(6).toLong) else None

    if (!Seq("random", "parallel").contains(init_mode)) {
      err.println(s"init-mode should be 'random' or 'parallel'. ${init_mode} is not supported.")
      return
    }

    if (!Seq("rdd", "dataset", "dataframe").contains(implementation_type)) {
      err.println(s"implemenatation-type should be 'rdd', 'dataset' or 'dataframe'. ${implementation_type} is not supported.")
    }

    implementation_type match {
      case "rdd" => KMeansRDD.run(dataset_path, init_mode, num_features, num_clusters, epsilon, seed)
      case "dataset" => KMeansDataSet.run(dataset_path, init_mode, num_features, num_clusters, epsilon, seed)
      case "dataframe" => KMeansDataFrame.run(dataset_path, init_mode, num_features, num_clusters, epsilon, seed)
    }
  }
}
