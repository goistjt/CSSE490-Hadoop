package edu.rosehulman.goistjt

object KeyValueTest extends App {
  if (args.length < 1) {
    System.err.println("Incorrect number of arguments. Require an input location")
    System.exit(-1)
  }
  val sc = new SparkContext(new SparkConf().setAppName("Spark KV Pairs"))
  val data = sc.textFile(args(0))
  data.foreach(println(_))
}
