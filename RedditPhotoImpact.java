package com.RUSpark;

/* any necessary Java packages here */

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class RedditPhotoImpact {
  private static final Pattern SPACE = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 

    SparkSession spark = SparkSession
      .builder()
      .appName("RedditPhotoImpact")
      .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
    List<Tuple2<Integer, Integer>> rList = new ArrayList<Tuple2<Integer, Integer>>();
    
    int i = 0;
    int impact = 0;
    int image_id = 0;
    for(String word:words.collect()){
      if (i%7 == 0) {
        image_id = Integer.parseInt(word);
      } else if (i%7 == 4) {
        impact += Integer.parseInt(word);
      } else if (i%7 == 5) {
        impact += Integer.parseInt(word);
      } else if (i%7 == 6) {
        impact += Integer.parseInt(word);
        rList.add(new Tuple2<Integer, Integer>(image_id, impact));
        impact = 0;
      }
      i++;
    }

    JavaRDD<Tuple2<Integer, Integer>> rRDD = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(rList);
    JavaPairRDD<Integer, Integer> pairRdd = JavaPairRDD.fromJavaRDD(rRDD);
    JavaPairRDD<Integer, Integer> counts = pairRdd.reduceByKey((i1, i2) -> i1 + i2).sortByKey();

    List<Tuple2<Integer, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + " " + tuple._2());
    }

    spark.stop();
	}
}
