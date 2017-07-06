package edu.rosehulman.goistjt

object Sparky {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage: sparky <dict_file> <content_file> <output_loc> <num_clusters> <num_iters>")
      System.exit(-1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("Sparky Vectorize"))
    val dict = sc.textFile(args(0)).map(_.split("\t")(0)).take(100)
    val content = sc.textFile(args(1))

    // Split each line into the URL, and text content
    val arrs = content.map((line) => line.split("\t"))
    val urlToContent = arrs.map(arr => {
      (arr(0), arr(1))
    })

    // Generate the word count for each URL
    val urlToCounts = urlToContent.map(tuple => {
      val url: String = tuple._1
      val text = tuple._2
      val counts: Map[String, Int] = text
        .toLowerCase()
        .replaceAll("[,.?/;:<>\"\\[\\{\\]\\}!@#$%^&*\\(\\)-_=+\\|]", " ")
        .trim
        .split("\\s+")
        .groupBy(a => a)
        .map(args => (args._1, args._2.length))
      (url, counts)
    })
    urlToCounts.cache()

    // Apply the dictionary to each word count for identical word vectors
    val urlDict = urlToCounts.map(tuple => {
      val vects = collection.mutable.Map.empty[String, Int]
      dict.foreach(word => {
        vects(word) = tuple._2.getOrElse(word, 0)
      })
      (tuple._1, vects)
    })
    urlToCounts.unpersist()

    // Convert word map to Vector of occurrences and store as DataFrame
    val urlToVector = urlDict.map(tuple => {
      (tuple._1, Vectors.dense(tuple._2.map(_._2.toDouble).toArray))
    })
    urlToVector.cache()

    val foo: DataFrame = SparkSession.builder()
      .getOrCreate()
      .createDataFrame(urlToVector).toDF("url", "features")
    urlToVector.unpersist()

    // Run KMeans clustering
    val kMeans = new KMeans()
      .setK(args(3).toInt)
      .setMaxIter(args(4).toInt)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val model = kMeans.fit(foo)


    // Convert back to RDD and save.
    sc.parallelize(model.summary.predictions.collect()).saveAsTextFile(args(2))
    //    val WSSSE = clusters.computeCost(rows)
    //    clusters.save(sc, args(2))

    //    urlDict.saveAsTextFile(args(2))
    sc.stop()
  }
}
