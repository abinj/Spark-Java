package com.abinj.sparkjava.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class ExplodeJSON {

    public static void main(String[] args) {
//        if (args.length < 1) {
//            System.out.println("Arguments required:- [input json file path]");
//        }
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("sparkbasicexamples")
                .getOrCreate();

        SQLContext sqlContext = new SQLContext(spark);
        Dataset<Row> peopleDF =
                sqlContext.read().format("json").load("/home/abin/my_space/java_backend/git_works/Spark-Java/sparkbasicexamples/src/main/resources/assets/explode_test_data.json");
        peopleDF.printSchema();
        peopleDF.show();


    }
}
