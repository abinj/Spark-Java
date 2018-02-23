package com.abinj.sparkjava.application;

import com.abinj.sparkjava.models.Person;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;


public class ParseJSON {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sparkbasicexamples").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(args[0]);
        JavaRDD<Person> result = input.map(stringInput -> new ObjectMapper().readValue(stringInput, Person.class));

        JavaRDD<String> formatted = result.map(jsonInput -> new ObjectMapper().writeValueAsString(jsonInput));

        formatted.saveAsTextFile(args[1]);
    }
}
