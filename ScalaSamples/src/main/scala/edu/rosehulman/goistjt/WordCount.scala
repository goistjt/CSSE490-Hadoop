package edu.rosehulman.goistjt

object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Incorrect number of arguments. Require an input and output location")
      System.exit(-1)
    }
    val sc = new SparkContext(new SparkConf().setAppName("Spark Word Count"))
    val txtFile = args(0)
    val textData = sc.textFile(txtFile)
    val tokenized = textData.flatMap(_.replaceAll("(,.;)", "").split(" "))
    val counts = tokenized.map((_, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))
    sc.stop()
  }
}
