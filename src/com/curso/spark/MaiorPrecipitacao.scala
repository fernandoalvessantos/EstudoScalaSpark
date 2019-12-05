package com.curso.spark

import org.apache.log4j._
import org.apache.spark._

import scala.math.max

/** Achar o dia de maior precipitação */
object MaiorPrecipitacao {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val station = fields(0)
    val data = fields(1)
    val entryType = fields(2)
    val precip = fields(3).toFloat
    (station, data, entryType, precip)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("com").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaiorPrecipitacao")
    val lines = sc.textFile("./1800.csv")
    val parsedLines = lines.map(parseLine)

    val precipitations = parsedLines.filter(x => x._3 == "PRCP")

    //val stationPreciptations = precipitations.map(x => (x._1, x._4.toFloat))
    val stationPreciptations = precipitations.map(x => (x._1, (x._2, x._4.toFloat)))
    stationPreciptations.collect().sorted.foreach(println)

    val maxPreci = stationPreciptations.reduceByKey((x, y) => {
      if (x._2 > y._2) {
        (x._1, x._2)
      } else {
        (y._1, y._2)
      }
    })

    maxPreci.collect().foreach(println)

  }
}