package com.abinj.sparkjava.application;

import com.abinj.sparkjava.models.AvgCount;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RDDPairAvg {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sparkbasicexamples").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*combineByKey(), Combine values with the same key using a different result type.*/
        List<Tuple2<String, Integer>> input = new ArrayList<>();
        input.add(new Tuple2<>("python", 1));
        input.add(new Tuple2<>("scala", 2));
        input.add(new Tuple2<>("java", 3));
        JavaPairRDD<String, Integer> pair = sc.parallelizePairs(input);

        Function<Integer, AvgCount> createAcc = x -> new AvgCount(x, 1);
        Function2<AvgCount, Integer, AvgCount> addAndCount =
                (AvgCount x, Integer y) -> new AvgCount(x.getTotal_() + y, x.getNum_() + 1);
        Function2<AvgCount, AvgCount, AvgCount> combine =
                (AvgCount x, AvgCount y) ->
                        new AvgCount(x.getTotal_() + y.getTotal_(), x.getNum_() + y.getNum_());

        JavaPairRDD<String, AvgCount> avg = pair.combineByKey(createAcc, addAndCount, combine);
        System.out.println("partitions: " + avg.partitions().size());
        Map<String, AvgCount> avgMap = avg.collectAsMap();
        for (Map.Entry<String, AvgCount> entry : avgMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().avg());
        }


        /*coGroup(), Group data from both RDDs sharing the same key.*/
        List<Tuple2<String, Integer>> input2 = new ArrayList<>();
        input2.add(new Tuple2<>("python", 4));
        input2.add(new Tuple2<>("scala", 2));
        JavaPairRDD<String, Integer> pair2 = sc.parallelizePairs(input2);
        JavaPairRDD<String, Integer> grouped = intersectByKey(pair, pair2);
        for (Tuple2<String, Integer> entry : grouped.collect()) {
            System.out.println(entry._1() + ": " + entry._2());
        }



        sc.stop();
    }


    public static <K, V> JavaPairRDD<K, V> intersectByKey(JavaPairRDD<K, V> rdd1, JavaPairRDD<K, V> rdd2) {
        JavaPairRDD<K, Tuple2<Iterable<V>, Iterable<V>>> grouped = rdd1.cogroup(rdd2);
        return grouped.flatMapValues((Function<Tuple2<Iterable<V>, Iterable<V>>, Iterable<V>>) input -> {
            ArrayList<V> al = new ArrayList<V>();
            if (!Iterables.isEmpty(input._1()) && !Iterables.isEmpty(input._2())) {
                Iterables.addAll(al, input._1());
                Iterables.addAll(al, input._2());
            }
            return al;
        });
    }


}
