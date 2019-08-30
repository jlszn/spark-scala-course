package com.part4

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.io.{Codec, Source}
import scala.math.sqrt

object MovieSimilarities extends Serializable {

  val ID = 180

  val lowestAcceptableRating = 3

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {
    // 1. Discard bad ratings
    // 2. Different similarity metrics
    // 3. Adjust the threshold
    // 4. Use genre in u.items to boost scores

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    val lines = Source.fromFile("../Spark/ml-100k/u.item").getLines()

    val movieNames = lines.map(line => line.split('|')).collect {
      case fields if fields.length > 1 => fields(0).toInt -> fields(1)
    }.toMap

    movieNames
  }

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def makePairs(userRatings: UserRatingPair): ((Int, Int), (Double, Double)) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  def filterDuplicates(userRatings: UserRatingPair): Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    movie1 < movie2
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    val score: Double =  if (denominator == 0) 0.0 else numerator / denominator

    (score, numPairs)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieSimilarities")

    println("\nLoading movie names...")
    val nameDict = loadMovieNames()

    val data = sc.textFile("../Spark/ml-100k/u.data")

    // Map ratings to key / value pairs: user ID => movie ID, rating
    val ratings: RDD[(Int, (Int, Double))] = data.map(l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    val filteredRatings: RDD[(Int, (Int, Double))] = ratings/*.filter(_._2._2 >= lowestAcceptableRating)*/

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    val joinedRatings: RDD[(Int, ((Int, Double), (Int, Double)))] = filteredRatings.join(filteredRatings)

    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

    // Filter out duplicate pairs
    val uniqueJoinedRatings: RDD[(Int, ((Int, Double), (Int, Double)))] = joinedRatings.filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs.groupByKey()

    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    //Save the results if desired
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-sims")

    // Extract similarities for the movie we care about that are "good".

    val scoreThreshold = 0.97
    val coOccurenceThreshold = 50.0

    // Filter for movies with this sim that are "good" as defined by
    // our quality thresholds above

    val filteredResults = moviePairSimilarities.filter(x => {
      val pair = x._1
      val sim = x._2
      (pair._1 == ID || pair._2 == ID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
    }
    )

    // Sort by quality score.
    val results: Array[((Double, Int), (Int, Int))] = filteredResults.map(x => (x._2, x._1)).sortByKey(false).take(10)

    println("\nTop 10 similar movies for " + nameDict(ID))
    for (result <- results) {
      val sim = result._1
      val pair = result._2
      // Display the similarity result that isn't the movie we're looking at
      var similarMovieID = pair._1
      if (similarMovieID == ID) {
        similarMovieID = pair._2
      }
      println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
    }
  }
}