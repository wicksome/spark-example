package me.wickso.spark.example

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Test

import scala.collection.mutable.ListBuffer

// 1.4.1절
class WordCountSpec {

  @Test
  def test() {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCountTest")
      .set("spark.local.ip", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")

    val sc = new SparkContext(conf)
    val input = new ListBuffer[String]
    input += "Apache Spark is a fast and general engine for large-scale data processing."
    input += "Spark runs on both Windows and UNIX-like systems"
    input.toList

    val inputRDD = sc.parallelize(input)
    val resultRDD = WordCount.process(inputRDD)
    val resultMap = resultRDD.collectAsMap

    assert(resultMap("Spark") == 2)
    assert(resultMap("and") == 2)
    assert(resultMap("runs") == 1)

    println(resultMap)

    sc.stop
  }
}
