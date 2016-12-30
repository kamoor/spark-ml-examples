package com.kamoor.examples.spark.ml.statistics

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/*

 * This sample code shows how to find pearson and spearman correlation of a set of co-ordinates.
 * Pass an s3 or hdfs location of the input file contains  one x,y pair per line
 *
 * To Submit:
 * spark-submit --class com.kamoor.examples.spark.ml.statistics.Correlation spark-ml-examples-1.0-SNAPSHOT.jar s3://kamoorr/xy.txt
 *
 * Sample xy.txt in resources/test-files/xy.txt
 *
 */

class CorrelationProcessor(sc: SparkContext) {



  def run(input: String) = {

    val xy = sc.textFile(input).map(line => line.split(","));
    xy.take(5);

    val xD = xy.map(v1 => v1(0).toDouble)
    val yD = xy.map(v1 => v1(1).toDouble)

    val corrPearson: Double = Statistics.corr(xD, yD, "pearson")
    val corrSpearman: Double = Statistics.corr(xD, yD, "spearman")

    println("---------------------------------- ")
    println("Pearson correlation  "+ corrPearson)
    println("Spearman correlation "+ corrPearson)
    println("---------------------------------- ")
  }

}

object Correlation {

  def main(args: Array[String]) {
    val input = args(0)
    println("Starting correlation calculation for "+ input)

    val conf = new SparkConf().setAppName("CorrelationKJob").setMaster("local")
    val context = new SparkContext(conf)
    val correlationProcessor = new CorrelationProcessor(context)
    correlationProcessor.run(input)
    context.stop()
    println("Ending Job");
  }
}