package com.part7

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD

object LinearRegressionRDD {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "LinearRegression")

    // This reads in x,y number pairs where x is the "label" we want to predict
    // and y is the "feature" which is some associated value we can use to predict the label by
    // Note that currently MLLib only works properly if this input data is first scaled such that
    // it ranges from -1 to 1 and has a mean of 0, more or less! You need to scale it down, then
    // remember to scale it back up later.
    val trainingLines: RDD[String] = sc.textFile("../Spark/regression.txt")

    // And another RDD containing our "test" data that we want to predict values for using our linear model.
    // This will expect both the known label and the feature data. In the real world you won't know
    // the "correct" value and would just input feature data.
    val testingLines: RDD[String] = sc.textFile("../Spark/regression.txt")

    // Convert input data to LabeledPoints for MLLib
    val trainingData: RDD[LabeledPoint] = trainingLines.map(LabeledPoint.parse).cache()
    val testData: RDD[LabeledPoint] = testingLines.map(LabeledPoint.parse)

    // Now we will create our linear regression model

    val algorithm: LinearRegressionWithSGD = new LinearRegressionWithSGD()
    algorithm.optimizer
      // 100 - 0.10066852907328923 norm
      // 200 - 0.10066852907328923 good
      // 1000 - 0.1343985333254639 good
      // 1 - 0.10024136639602943 a little worse
      .setNumIterations(1)
      // 1.0 - 0.10066852907328923 norm
      // 2.0 - 0.10073103047720411 worse
      // 0.5 - 0.10188046240656362 ?
      // 0.1 - 0.1858145809332145 good
      .setStepSize(1.0)
      .setUpdater(new SquaredL2Updater())
      // 0.01 - 0.10066852907328923 norm
      // 0.05 - 0.11057945309877877 worse
      // 0.001 - 0.1002436012823037 good
      .setRegParam(0.1)

    val model: LinearRegressionModel = algorithm.run(trainingData)

    // Compute total squared error:

    // keep track of the difference between the predicted value and the actual value
    // and take the square and add it all together
    // and divide that by how many data points you had

    // then play with optimizer values

    // Predict values for our test feature data using our linear regression model
    val predictions: RDD[Double] = model.predict(testData.map(_.features))

    // Zip in the "real" values so we can compare them
    val predictionAndLabel: RDD[(Double, Double, Double)] =
      predictions
        .zip(testData.map(_.label))
        .map(p => (p._1, p._2, Math.pow(p._1 - p._2, 2)))
        .cache()

    val len: Long = predictionAndLabel.count()

    // Print out the predicted and actual values for each point
    val sum: Double = {
      for (
        prediction <- predictionAndLabel
      ) yield {
        println(s"prediction: ${prediction._1}, real: ${prediction._2}, error: ${prediction._3}")
        prediction._3
      }
    }.collect().sum

    val res = Math.sqrt(sum / len)

    println(res)

  }
}