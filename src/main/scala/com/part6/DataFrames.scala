package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
    
object DataFrames {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    person
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val lines: RDD[String] = spark.sparkContext.textFile("../fakefriends.csv")
    val people: Dataset[Person] = lines.map(mapper).toDS().cache()
    
    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")
    
    println("Here is our inferred schema:")
    people.printSchema()
    
    println("Let's select the name column:")
    val l: DataFrame = people.select("name")
    
    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()
   
    println("Group by age:")
    people.groupBy("age").count().show()
    
    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()
    
    spark.stop()
  }
}