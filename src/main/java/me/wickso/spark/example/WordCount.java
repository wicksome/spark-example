package me.wickso.spark.example;

import org.apache.commons.lang3.ArrayUtils;

public class WordCount {
	public static void main(String[] args) {
		if (ArrayUtils.getLength(args) != 3) {
			System.err.println("Usage: WordCount <Master> <Input> <Output>");
			return;
		}
	}
}
