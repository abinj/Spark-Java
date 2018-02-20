package com.abinj.sparkjava.application;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class RDDPersist {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sparkbasicexamples").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> file = sc.textFile(args[0]);

        //Here we are doing 2 transformations, first one is filter out lines with length less than 5
        //And the second one is filter out lines which haven't keyword "persist".
        JavaRDD<String> lines = file.filter(line -> line.length() > 5)
                .filter(line -> line.contains("persist"));

        //Here we persist the second transformation to memory
        //ie, for the further actions the RDD wouldn't recompute the transformation 1
        lines.persist(StorageLevel.MEMORY_ONLY());

        System.out.println("Total no of lines:- " + lines.count());
        System.out.println("First line:- " + lines.first());
        sc.stop();
    }
}
