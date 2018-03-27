package com.abinj.sparkjava.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.count;

public class ParseCSV {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("sparkbasicexamples")
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(spark);

        Dataset<Row> csvRdd = spark.read()
                .option("mode", "DROPMALFORMED")
                .option("header", true)
                .csv("/home/abin/my_space/java_backend/git_works/Spark-Java/sparkbasicexamples/src/main/resources/assets/iris.csv");
        csvRdd.printSchema();
        csvRdd.show();

        Dataset<Row> dfResult = csvRdd.groupBy("species").agg(count("species"));
        dfResult.printSchema();
        dfResult.show();
    }
}
