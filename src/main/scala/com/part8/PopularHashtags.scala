package com.part8

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter._
import twitter4j.Status

import scala.io.Source

/** Listens to a stream of Tweets and keeps track of the most popular
 * hashtags over a 5 minute window.
 */
object PopularHashtags {

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger: Logger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twitter.txt in the main workspace directory */
  def setupTwitter(): Unit = {
    import scala.io.Source

    for (line <- Source.fromFile("../Spark/twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  def bannedWords = {
    val words: Iterator[String] = Source.fromFile("../Spark/stop-words.txt").getLines

    val banned =
      words
        .map(line => line.split('\n'))
        .collect {
          case fields if fields.nonEmpty => fields(0)
        }

    banned.toList
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // CHALLENGE
    // most popular word
    // average length of a tweet

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses: DStream[String] = tweets.map(status => status.getText)

    // Now count them up over a 5 minute window sliding every one second
    val hashtagCounts = statuses.map(_.length)

    // Sort the results by the count values
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x, false))

    // Print the top 10
    sortedResults.print

    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("/home/jsizon/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
