package com.abinj.sparkjava.application;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/** Read the files and count the occurrences of a specific word
 * arg_1 is input file path
 * arg_2 is output file path
 * **/
public class WordCount {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Required arguments missing!!!");
            return;
        }
        SparkConf conf = new SparkConf().setAppName("word-count").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> file = sc.textFile(args[0]);

        JavaRDD<String> words = file.flatMap(
                (FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                (PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        for (Tuple2<?, ?> tuple : counts.collect()) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        if (args.length == 2) {
            counts.saveAsTextFile(args[1]);
        }
        sc.stop();
    }
}
