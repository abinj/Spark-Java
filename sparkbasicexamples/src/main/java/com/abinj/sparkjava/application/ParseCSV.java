package com.abinj.sparkjava.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.count;

public class ParseCSV {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Arguments required:- [input csv file path]");
        }
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("sparkbasicexamples")
                .getOrCreate();

        Dataset<Row> csvRdd = spark.read()
                .option("mode", "DROPMALFORMED")
                .option("header", true)
                .csv(args[0]);
        csvRdd.printSchema();
        csvRdd.show();

        Dataset<Row> dfResult = csvRdd.groupBy("species").count();
        dfResult.printSchema();
        dfResult.show();
    }
}
