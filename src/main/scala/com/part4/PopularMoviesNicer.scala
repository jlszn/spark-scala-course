package com.part4

import java.nio.charset.CodingErrorAction

import com.part6.PopularMoviesDataSets.loadMovieNames
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{Dataset, RelationalGroupedDataset, Row, SparkSession}

import scala.io.{Codec, Source}

/** Find the movies with the most ratings. */
object PopularMoviesNicer {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val lines = Source.fromFile("./src/main/resources/ml-100k/u.item").getLines()

    // Create a Map of Ints to Strings, and populate it from u.item.
    val movieNames: Map[Int, String] =
      lines
        .map(line => line.split('|'))
        .collect {
          case fields if fields.nonEmpty => (fields(0).toInt, fields(1))
        }.toMap

    movieNames
  }

  final case class Movie(id: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    // Read in each rating line
    val lines: RDD[Movie] =
      spark.sparkContext.textFile("./src/main/resources/ml-100k/u.data")
        .map(x => Movie(x.split("\t")(1).toInt))

    import spark.implicits._
    val moviesDS: Dataset[Movie] = lines.toDS()

    val topMovies: Array[Row] = moviesDS.groupBy("id").count().orderBy(desc("count")).take(10)

    val ids: Array[Int] = topMovies.map(n => n(0).asInstanceOf[Int])

    val topNames: Array[String] = ids.flatMap(id => loadMovieNames().get(id))

    topNames.foreach(d => println(d))

    spark.stop
  }

}

