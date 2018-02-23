package com.abinj.sparkjava.application;

import com.abinj.sparkjava.models.Person;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class ParseJSON {

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("arguments needs: [input_json_file] [output_json_file1] [output_json_file2]");
        }

        //Example_1, read json file and parse it into java model then write back json string
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("sparkbasicexamples")
                .getOrCreate();
        RDD<String> input = sparkSession.sparkContext().textFile(args[0], 2);
        JavaRDD<Person> result = input.toJavaRDD().map(stringInput -> new ObjectMapper().readValue(stringInput, Person.class));
        JavaRDD<String> formatted = result.map(jsonInput -> new ObjectMapper().writeValueAsString(jsonInput));
        formatted.saveAsTextFile(args[1]);



        //Example_2, read json file into sql schema
        StructType schema = new StructType( new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.StringType, true),
                DataTypes.createStructField("gender", DataTypes.StringType, true)});

        Dataset<Row> peoplesSchema = sparkSession.read().schema(schema).json(args[0]);
        peoplesSchema.printSchema();
        peoplesSchema.show();

        peoplesSchema.write().format("json").mode("overwrite").save(args[2]);


        sparkSession.stop();
    }
}
