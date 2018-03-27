package com.abinj.sparkjava.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParseCSV {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("sparkbasicexamples")
                .getOrCreate();

        Dataset<Row> csvRdd = spark.read()
                .option("header", true).format("csv")
                .load("/home/abin/my_space/java_backend/git_works/Spark-Java/sparkbasicexamples/src/main/resources/assets/iris.csv");
        csvRdd.printSchema();
        csvRdd.show();
    }
}
