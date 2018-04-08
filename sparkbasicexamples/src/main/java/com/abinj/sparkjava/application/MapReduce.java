package com.abinj.sparkjava.application;

import com.abinj.sparkjava.models.Slots;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class MapReduce {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Arguments required:- [input json file path]");
        }
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("sparkbasicexamples")
                .getOrCreate();

        Dataset<Row> parkLogData = sparkSession.read().schema(Slots.SCHEMA).json(args[0]);
        parkLogData.printSchema();
        parkLogData.show();

        Dataset<Row> filteredData = filterOutOldDuplicateData(parkLogData, sparkSession);
        filteredData.printSchema();
        filteredData.show();
    }

    private static Dataset<Row> filterOutOldDuplicateData(Dataset<Row> parkLogData, SparkSession sparkSession) {
        JavaPairRDD<Long, Row> mappedPairs = parkLogData.toJavaRDD().mapToPair((row) -> {
            long key = row.getLong(row.fieldIndex("id"));
            return Tuple2.apply(key, row);
        });

        JavaPairRDD<Long, Row> reducedPair = mappedPairs.reduceByKey((row1, row2) -> {
            String date1 = row1.getString(row1.fieldIndex("exit_timestamp"));
            String date2 = row2.getString(row2.fieldIndex("exit_timestamp"));
            if (date1.compareTo(date2) >= 0) {
                return row1;
            } else {
                return row2;
            }
        });

        JavaRDD<Row> filteredData = reducedPair.values();
        return sparkSession.createDataFrame(filteredData, Slots.SCHEMA);
    }


}
