package com.part3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/** Find the minimum temperature by weather station */
object MinTemperatures extends Serializable {

  // CHALLENGE: precipitation

  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toInt/*.toFloat * 0.1f * (9.0f / 5.0f) + 32.0f*/
    (stationID, entryType, temperature)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    // Read each line of input data
    val lines = sc.textFile("../../Downloads/SparkScala/1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines: RDD[(String, String, Int)] = lines.map(parseLine)

    // Filter out all but TMIN entries
    val prcps: RDD[(String, String, Int)] = parsedLines.filter(x => x._2 == "PRCP")

    // Convert to (stationID, temperature)
    val stationTemps: RDD[(String, Int)] = prcps.map(x => (x._1, x._3.toInt))

    // Reduce by stationID retaining the minimum temperature found
    val maxPercpByStation: RDD[(String, Int)] = stationTemps.reduceByKey((x, y) => Math.max(x,y))

    // Collect, format, and print the results
    val results = maxPercpByStation.collect()

    for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%d"
       println(s"$station max precipitation: $formattedTemp")
    }

  }
}
