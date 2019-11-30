package me.wickso.spark.example;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		if (ArrayUtils.getLength(args) != 3) {
			System.err.println("Usage: WordCount <Master> <Input> <Output>");
			return;
		}

		try (JavaSparkContext sc = getSparkContext("WordCount", args[0]);) {
			JavaRDD<String> inputRDD = getInputRDD(sc, args[1]);

			JavaPairRDD<String, Integer> resultRDD = process(inputRDD);

			handleResult(resultRDD, args[2]);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static JavaSparkContext getSparkContext(String appName, String master) {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		return new JavaSparkContext(conf);
	}

	private static JavaRDD<String> getInputRDD(JavaSparkContext sc, String input) {
		return sc.textFile(input);
	}

	static JavaPairRDD<String, Integer> process(JavaRDD<String> inputRDD) {
		return inputRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
			.mapToPair(w -> new Tuple2<>(w, 1))
			.reduceByKey(Integer::sum);
	}

	private static void handleResult(JavaPairRDD<String, Integer> resultRDD, String output) {
		resultRDD.saveAsTextFile(output);
	}
}
