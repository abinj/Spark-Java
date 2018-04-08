package com.abinj.sparkjava.application;

import org.apache.spark.sql.*;

public class ExplodeJSON {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Arguments required:- [input json file path]");
        }
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("sparkbasicexamples")
                .getOrCreate();

        SQLContext sqlContext = new SQLContext(spark);
        Dataset<Row> peopleDF =
                sqlContext.read().format("json").load(args[0]);
        peopleDF = peopleDF.select(peopleDF.col("Name"), peopleDF.col("Email"), peopleDF.col("Designation")
                , peopleDF.col("Age"), peopleDF.col("location"), peopleDF.col("Company")
                , org.apache.spark.sql.functions.explode(peopleDF.col("Test")).as("Test"));
        peopleDF.printSchema();
        peopleDF.show();


    }
}
