package com.part3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CustomerSpent extends Serializable {

  // count how much each customer has spent

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "WordCountBetterSorted")

    // Load each line of my book into an RDD
    val input: RDD[String] = sc.textFile("src/main/resources/customer-orders.csv")

    def parseLine(line: String) = {
      val fields = line.split(",")

      val customer = fields(0).toInt
      val spent = fields(2).toFloat

      (customer, spent)
    }

    val customersToSpent: RDD[(Int, Float)] = input.map(parseLine)

    val cusSums: RDD[(Int, Float)] = customersToSpent.reduceByKey(_ + _)

    val sorted = cusSums.map(c => (c._2, c._1)).sortByKey()

    sorted.collect().foreach(c => println("sum: " + c._1 + ", id:" + c._2))

  }
}
