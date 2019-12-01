package me.wickso.spark.example

import me.wickso.spark.example.WordCount.process
import org.apache.commons.lang3.ArrayUtils
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2

fun main(args: Array<String>) {
    if (ArrayUtils.getLength(args) != 3) {
        System.err.println("Usage: WordCount <Master> <Input> <Output>")
        return
    }
    try {
        getSparkContext("WordCount", args[0]).use { sc ->
            val inputRDD = getInputRDD(sc, args[1])
            val resultRDD = process(inputRDD)
            handleResult(resultRDD, args[2])
        }
    } catch (e: Exception) {
        e.printStackTrace()
    }
}

private fun getSparkContext(appName: String, master: String): JavaSparkContext {
    val conf = SparkConf().setAppName(appName).setMaster(master)
    return JavaSparkContext(conf)
}

private fun getInputRDD(sc: JavaSparkContext, input: String): JavaRDD<String> {
    return sc.textFile(input)
}

fun process(inputRDD: JavaRDD<String>): JavaPairRDD<String, Int> {
    return inputRDD.flatMap { s: String -> listOf(*s.split(" ".toRegex()).toTypedArray()).iterator() }
            .mapToPair { w: String -> Tuple2(w, 1) }
            .reduceByKey { a: Int?, b: Int? -> Integer.sum(a!!, b!!) }
}

private fun handleResult(resultRDD: JavaPairRDD<String, Int>, output: String) {
    resultRDD.saveAsTextFile(output)
}
