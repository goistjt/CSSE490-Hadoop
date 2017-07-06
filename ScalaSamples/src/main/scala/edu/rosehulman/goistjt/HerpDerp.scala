package edu.rosehulman.goistjt

object HerpDerp {
  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println("Usage: sparky <dict_file> <content_file> <output_loc> <num_clusters> <num_iters> <dict_size>")
      System.exit(-1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("Sparky Vectorize"))
    val dict = sc.textFile(args(0)).map(_.split("\t")).sortBy(arr => arr(1)).take(args(5).toInt)
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
      val vects = collection.mutable.Map.empty[String, Double]
      dict.foreach(wordCount => {
        vects(wordCount(0)) = tuple._2.getOrElse(wordCount(0), 0).toDouble / wordCount(1).toDouble
      })
      (tuple._1, vects)
    })
    urlToCounts.unpersist()

    // Convert word map to Vector of occurrences and store as DataFrame
    val urlToVector = urlDict.map(tuple => {
      (tuple._1, Vectors.dense(tuple._2.map(_._2.toDouble).toArray))
    })

    // Run KMeans clustering
    val clusters = KMeans.train(urlToVector.values, args(3).toInt, args(4).toInt)

    val fin = urlToVector.map(tuple => {
      (tuple._1, clusters.predict(tuple._2))
    })
    fin.saveAsTextFile(args(2))
    // Convert back to RDD and save.
    //    val WSSSE = clusters.computeCost(rows)
    //    clusters.save(sc, args(2))

    //    urlDict.saveAsTextFile(args(2))
    sc.stop()
  }
}
