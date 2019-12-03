package com.curso.spark

import java.io.Serializable

import org.apache.log4j._
import org.apache.spark._

/** Compute the average number of friends by name in a social network. */
object NomePorIdade {

  /** A function that splits a line of input into (name, numFriends) tuples. */
  def parseLine2(line: String) = {
    val fields = line.split(",")
    val name = fields(1).toString
    val numFriends = fields(3).toInt

    (name, numFriends)
  }


  def main(args: Array[String]) {

    Logger.getLogger("com").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "NomePorIdade")

    val lines = sc.textFile("./fakefriends.csv")
    val rdd = lines.map(parseLine2)
    val nomePorQuantidade = rdd.mapValues(x => (x, 1))


    nomePorQuantidade.collect().sorted.foreach(println)

    val totalizador = nomePorQuantidade.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))


    totalizador.collect().sorted.foreach(println)

    val media = totalizador.map(x => (x._1, (x._2._1 / x._2._2)))

    media.collect().sorted.foreach(println)
  }

}
  