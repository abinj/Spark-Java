package com.abinj.sparkjava.application;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class RDDBasicActions {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sparkbasicexamples").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("collect(), return all elements from the RDD");
        if (args.length> 0) {
            JavaRDD<String> file = sc.textFile(args[0]);
            JavaRDD<String> lines = file.filter(line -> line.contains("RDD"));
            lines.collect().forEach(line -> System.out.println(line));

            System.out.println("count(), return number of elements in the RDD");
            System.out.println("Number of lines:- " + lines.count());
        }

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));

        System.out.println("countByValue(), number of times each element occurs in the RDD.");
        rdd.countByValue().forEach((value, count) -> System.out.println(value + ": " + count));

        System.out.println("take(num), Return num elements from RDD");
        rdd.take(2).forEach(item -> System.out.println(item));

        System.out.println("top(num), Return the top num elements from RDD");
        rdd.top(2).forEach(item -> System.out.println(item));

        System.out.println("takeOrdered(num)(ordering), Return num elements based on provided ordering.");
        rdd.takeOrdered(2).forEach(item -> System.out.println(item));

        System.out.println("takeSample(withReplacement, num, [seed]), Return num elements at random.");
        rdd.takeSample(false, 5).forEach(item -> System.out.println(item));

        System.out.println("reduce(func), Combine the elements of the RDD together in parallel (e.g., sum ).");
        System.out.println("sum: " + rdd.reduce((i1, i2) -> i1 + i2));

        System.out.println("fold(zero)(func), Same as reduce() but with a provided 0 value");
        System.out.println("sum: " + rdd.fold(1, (i1, i2) -> i1 + i2));

        System.out.println("foreach(func), Apply the provided function to each element of the RDD.");
        rdd.foreach(input -> System.out.println(input));

        sc.stop();
    }
}
