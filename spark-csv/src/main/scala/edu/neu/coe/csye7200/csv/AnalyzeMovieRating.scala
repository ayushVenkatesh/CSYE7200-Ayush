package edu.neu.coe.csye7200.csv
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._


case class Analyze_movie_rating() {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Mean and std_dev of movie rating")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val df: DataFrame = spark.read.option("header", "true").csv("/movie_metadata.csv")
  val df_rating: DataFrame = df.select("imdb_score")
  val mean: Long = df_rating.select(avg(df_rating("imdb_score"))).first().getLong(0)
  val std_dev_rating: Long = df_rating.select(stddev(df_rating("imdb_score"))).first().getLong(0)
  val count: Long = df_rating.count()
}

object Analyze_movie_rating extends App {
  Analyze_movie_rating()
}
