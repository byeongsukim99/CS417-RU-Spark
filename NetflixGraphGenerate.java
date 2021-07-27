package com.RUSpark;

/* any necessary Java packages here */

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;

import java.util.List;
import java.util.regex.Pattern;
import java.util.LinkedList;

public class NetflixGraphGenerate {

	public static void main(String[] args) throws Exception {
    final Pattern SPACE = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
      .builder()
      .appName("NetflixGraphGenerate")
      .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

    // unique list of (movie, rating) pairs
    JavaPairRDD<Integer, Integer> rPairRDD = lines.mapToPair(t -> new Tuple2<>( Integer.parseInt(SPACE.split(t)[0]) , Integer.parseInt(SPACE.split(t)[2]) )).distinct();
    // list of unique(by structure) ((movie, rating), customer) pairs
    JavaPairRDD<Tuple2<Integer, Integer>, Integer> cPairRDD = lines.mapToPair(t -> new Tuple2<>( new Tuple2<>(Integer.parseInt(SPACE.split(t)[0]), Integer.parseInt(SPACE.split(t)[2])), Integer.parseInt(SPACE.split(t)[1]) ));

    List<Tuple2<Tuple2<Integer, Integer>, Integer>> edges = new LinkedList<Tuple2<Tuple2<Integer, Integer>, Integer>>();
    // for each (movie, rating) pair
    for( Tuple2<Integer,Integer> tuple : rPairRDD.collect()){
      // find list of customers w/ (movie, rating) pair
      Function<Tuple2<Tuple2<Integer, Integer>, Integer>, Boolean> filterFunction = w -> (w._1 == tuple);
      JavaRDD<Integer> cidRDD = cPairRDD.filter(filterFunction).values();
      // pair all these nodes together
      Function<Tuple2<Integer, Integer>, Boolean> filterFunction2 = w -> (w._1 < w._2);
      JavaPairRDD<Tuple2<Integer, Integer>, Integer> combo = cidRDD.cartesian(cidRDD).filter(filterFunction2).mapToPair(s -> new Tuple2<>(s, 1));
      List<Tuple2<Tuple2<Integer, Integer>, Integer>> addon = combo.collect();
      edges.addAll(addon);
    }

    JavaRDD<Tuple2<Tuple2<Integer, Integer>, Integer>> fRDD = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(edges);
    JavaPairRDD<Tuple2<Integer, Integer>, Integer> fPairRdd = JavaPairRDD.fromJavaRDD(fRDD);
    JavaPairRDD<Tuple2<Integer, Integer>, Integer> result = fPairRdd.reduceByKey((i1, i2) -> i1 + i2);

    List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = result.collect();

    for(Tuple2<Tuple2<Integer, Integer>, Integer> tuple : output) {
      System.out.println("(" + tuple._1()._1() + "," + tuple._1()._2() + ") " + tuple._2());
    }
    
    spark.stop();
	}

}
