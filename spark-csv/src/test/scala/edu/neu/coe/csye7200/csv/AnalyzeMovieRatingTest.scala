package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.Dataset
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.Try

class AnalyzeMovieRatingTest extends AnyFlatSpec with Matchers {

  behavior of "Analyze_movie_rating"
  it should "get mean and stddev" in {
    val mean = Analyze_movie_rating().mean
    val stddev = Analyze_movie_rating().std_dev_rating
    val count = Analyze_movie_rating().count

    /*Rounding mean and stddev to two decimal places */
    mean - (mean % 0.01) shouldBe 6.45
    stddev - (stddev % 0.01) shouldBe 0.99
    count shouldBe 1609
  }

}
