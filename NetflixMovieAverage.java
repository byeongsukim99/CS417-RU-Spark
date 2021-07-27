package com.RUSpark;

/* any necessary Java packages here */

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.regex.Pattern;
import java.math.RoundingMode;
import java.text.DecimalFormat;

public class NetflixMovieAverage {
  private static final Pattern SPACE = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
      .builder()
      .appName("NetflixMovieAverage")
      .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

    JavaPairRDD<Integer, Double> rRDD = lines.mapToPair(t -> new Tuple2<>( Integer.parseInt(SPACE.split(t)[0]) , Double.parseDouble(SPACE.split(t)[2]) ));

    JavaPairRDD<Integer, Double> cRDD = rRDD.keys().mapToPair(s -> new Tuple2<>(s, 1.0)).reduceByKey((i1, i2) -> i1 + i2).sortByKey();
    JavaPairRDD<Integer, Double> counts = rRDD.reduceByKey((i1, i2) -> i1 + i2).sortByKey();

    JavaPairRDD<Integer, Double> divide = cRDD.join(counts).mapToPair(t -> new Tuple2<>(t._1(), t._2()._2() / (t._2()._1()) )).sortByKey();
    
    List<Tuple2<Integer, Double>> output = divide.collect();
    DecimalFormat decimalFormat = new DecimalFormat("#.##");
    decimalFormat.setRoundingMode(RoundingMode.FLOOR);
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + " " + decimalFormat.format(tuple._2()) );
    }

    spark.stop();
	}

}
