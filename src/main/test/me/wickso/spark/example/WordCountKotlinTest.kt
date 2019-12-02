package me.wickso.spark.example

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

class WordCountKotlinTest {
    companion object {
        private var conf: SparkConf? = null
        private var sc: JavaSparkContext? = null

        @BeforeAll
        @JvmStatic
        fun setup() {
            conf = SparkConf()
                    .setAppName("WordCountTest")
                    .setMaster("local[*]")
                    .set("spark.local.ip", "127.0.0.1")
                    .set("spark.driver.host", "127.0.0.1")
            sc = JavaSparkContext(conf)
        }

        @AfterAll
        @JvmStatic
        fun cleanup() {
            if (sc != null) {
                sc!!.stop()
            }
        }
    }

    @Test
    fun `Spark example test`() {
        // given
        val inputRDD = sc!!.parallelize(listOf(
                "Apache Spark is a fast and general engine for large-scale data processing.",
                "Spark runs on both Windows and UNIX-like systems"
        ))
        // when
        val resultRDD = WordCount.process(inputRDD)
        val resultMap = resultRDD.collectAsMap()
        // then
        Assertions.assertAll(
                Executable { Assertions.assertEquals(2, resultMap["Spark"]) },
                Executable { Assertions.assertEquals(2, resultMap["and"]) },
                Executable { Assertions.assertEquals(1, resultMap["runs"]) }
        )
        println(resultMap)
    }
}
