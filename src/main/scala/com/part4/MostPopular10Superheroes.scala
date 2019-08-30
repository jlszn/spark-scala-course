package com.part4

import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** Find the superhero with the most co-appearances. */
object MostPopular10Superheroes extends Serializable {

  case class Hero(id: Int, name: String)

  case class Cooccurances(id: Int, numOfConnections: Int)

  // Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String): Cooccurances = {
    val elements = line.split("\\s+")
    Cooccurances(elements(0).toInt, elements.length - 1)
  }

  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String): Option[Hero] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      Some(Hero(fields(0).trim().toInt, fields(1)))
    } else {
      None // flatmap will just discard None results, and extract data from Some results.
    }
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    // Build up a hero ID -> name RDD
    val names: RDD[String] = spark.sparkContext.textFile("../../Downloads/SparkScala/Marvel-names.txt")
    val lines: RDD[String] = spark.sparkContext.textFile("../../Downloads/SparkScala/Marvel-graph.txt")

    import spark.implicits._

    val namesDs: Dataset[String] = names.toDS()
    val linesDs: Dataset[String] = lines.toDS()

    val namesParsed: Dataset[Hero] = namesDs.flatMap(parseNames)
    val pairings: Dataset[Cooccurances] = linesDs.map(countCoOccurences)

    // heroId -> numOfConnections
    val totalFriendsByCharacter: Dataset[Cooccurances] =
      pairings
        .groupBy("id")
        .sum("numOfConnections")
        .map(row => Cooccurances(row.getInt(0), row.getLong(1).toInt))
        .cache()

    val k: Array[Row] =
      totalFriendsByCharacter
        .join(namesParsed, "id")
        .orderBy(org.apache.spark.sql.functions.col("numOfConnections").desc)
        .take(10)

    k.foreach(println)

  }

}
