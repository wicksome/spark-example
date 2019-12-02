package me.wickso.spark.example;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WordCountTest {
	private static SparkConf conf;
	private static JavaSparkContext sc;

	@BeforeAll
	static void setup() {
		conf = new SparkConf().setAppName("WordCountTest").setMaster("local[*]");
		conf.set("spark.local.ip", "127.0.0.1");
		conf.set("spark.driver.host", "127.0.0.1");
		sc = new JavaSparkContext(conf);
	}

	@DisplayName("Spark 예제 테스트")
	@Test
	void testProcess() {
		// given
		JavaRDD<String> inputRDD = sc.parallelize(Arrays.asList(
			"Apache Spark is a fast and general engine for large-scale data processing.",
			"Spark runs on both Windows and UNIX-like systems"
		));

		// when
		JavaPairRDD<String, Integer> resultRDD = WordCountJava.process(inputRDD);
		Map<String, Integer> resultMap = resultRDD.collectAsMap();

		// then
		assertAll(
			() -> assertEquals(2, resultMap.get("Spark")),
			() -> assertEquals(2, resultMap.get("and")),
			() -> assertEquals(1, resultMap.get("runs"))
		);
		System.out.println(resultMap);
	}

	@AfterAll
	static void cleanup() {
		if (sc != null) {
			sc.stop();
		}
	}
}
