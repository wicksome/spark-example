package me.wickso.spark.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Usage: WordCount <Master> <Input> <Output>")

    val sc = getSparkContext("WordCount", args(0))
    val inputRDD = getInputRDD(sc, args(1))
    val resultRDD = process(inputRDD)

    handleResult(resultRDD, args(2))
  }

  private def getSparkContext(appName: String, master: String) = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    new SparkContext(conf)
  }

  private def getInputRDD(sc: SparkContext, input: String) = sc.textFile(input)

  def process(inputRDD: RDD[String]) = inputRDD
    .flatMap(str => str.split(" "))
    .map((_, 1))
    .reduceByKey(_ + _)

  private def handleResult(resultRDD: RDD[(String, Int)], output: String) {
    resultRDD.saveAsTextFile(output);
  }
}
