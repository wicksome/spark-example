package me.wickso.spark.example

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Test

class Chapter2Specs {

  @Test
  def example_2_19() {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCountTest")
      .set("spark.local.ip", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")

    val sc = new SparkContext(conf)

    val fruits = List("apple,orange", "grape,apple,mango", "blueberry,tomato,orange")
    val rdd = sc.parallelize(fruits)
      .flatMap(_.split(","))
      .collect

    print(rdd.mkString(", "))

    sc.stop()
  }

  @Test
  def example_2_22() {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCountTest")
      .set("spark.local.ip", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")

    val sc = new SparkContext(conf)

    val fruits = List("apple,orange", "grape,apple,mango", "blueberry,tomato,orange")
    val rdd1 = sc.parallelize(fruits)
    val rdd2 = rdd1.flatMap(log => {
      if (log.contains("apple")) {
        Some(log.indexOf("apple"))
      } else {
        None
      }
    })

    println(rdd2.collect().mkString(", "))

    sc.stop()
  }

  @Test
  def example_2_23() {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCountTest")
      .set("spark.local.ip", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")

    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 10, 3)
    val rdd2 = rdd1.mapPartitions(numbers => {
      print("DB연결 !!!")
      numbers.map {
        number => number + 1
      }
    })

    println(rdd2.collect.mkString(", "))

    sc.stop()
  }

  @Test
  def example_2_26() {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCountTest")
      .set("spark.local.ip", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")

    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 10, 3)
    val rdd2 = rdd1.mapPartitions(numbers => {
      print("DB연결 !!!")
      numbers.map {
        number => number + 1
      }
    })

    println(rdd2.collect.mkString(", "))

    sc.stop()
  }

  @Test
  def example_2_109() {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCountTest")
      .set("spark.local.ip", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")

    val sc = new SparkContext(conf)

    println(sc.parallelize(1 to 10, 3).reduce(_ + _))

    sc.stop()
  }

  @Test
  def example_2_115() {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCountTest")
      .set("spark.local.ip", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(Prod(300), Prod(200), Prod(100)), 10)

    val r1 = rdd.reduce((p1, p2) => {
      p1.price += p2.price
      p1.cnt += 1
      p1
    })
    println(s"Reduce: (${r1.price}, ${r1.cnt})")

    val r2 = rdd.fold(Prod(0))((p1, p2) => {
      p1.price += p2.price
      p1.cnt += 1
      p1
    })
    println(s"Fold: (${r2.price}, ${r2.cnt})")

    sc.stop()
  }
}

case class Prod(var price: Int) {
  var cnt = 1
}
