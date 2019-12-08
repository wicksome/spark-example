package me.wickso.spark.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

public class Chapter2Examples {
	private static JavaSparkContext sc;

	@BeforeAll
	static void setup() {
		final SparkConf conf = new SparkConf().setAppName("WordCountTest").setMaster("local[*]");
		conf.set("spark.local.ip", "127.0.0.1");
		conf.set("spark.driver.host", "127.0.0.1");
		sc = new JavaSparkContext(conf);
	}

	@AfterAll
	static void cleanup() {
		if (sc != null) {
			sc.stop();
		}
	}

	@Test
	void test1() {
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
		JavaRDD<Integer> rdd2 = rdd1.mapPartitionsWithIndex((idx, numbers) -> {
			List<Integer> result = new ArrayList<>();
			if (idx == 1) {
				numbers.forEachRemaining(result::add);
			}
			return result.iterator();
		}, true);
		System.out.println(rdd1.collect());
		System.out.println(rdd2.collect());
	}

	@Test
	void test2() {
		JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
		JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(elem -> new Tuple2<>(elem, 1))
			.mapValues(value -> value + 1);

		System.out.println(rdd2.collect());
	}

	@Test
	void test3() {
		List<Tuple2<Integer, String>> data = Arrays.asList(new Tuple2(1, "a,b"), new Tuple2(2, "a,c"), new Tuple2(1, "d,e"));
		JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(data);
		System.out.println(rdd1.collect()); // [(1,a,b), (2,a,c), (1,d,e)]

		JavaPairRDD<Integer, String> rdd2 = rdd1.flatMapValues(str -> Arrays.asList(str.split(",")));
		System.out.println(rdd2.collect()); // [(1,a), (1,b), (2,a), (2,c), (1,d), (1,e)]

		JavaPairRDD<Integer, Iterable<String>> rdd3 = rdd2.groupByKey();
		System.out.println(rdd3.collect()); // [(1,[a, b, d, e]), (2,[a, c])]
	}

	@Test
	void test4() {
		List<Tuple2<String, String>> data1 = Arrays.asList(new Tuple2("k1", "v1"), new Tuple2("k2", "v2"), new Tuple2("k1", "v3"));
		List<Tuple2<String, String>> data2 = Arrays.asList(new Tuple2("k1", "v4"));

		JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(data1);
		JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(data2);

		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> result = rdd1.cogroup(rdd2);

		System.out.println(result.collect());
	}
}
