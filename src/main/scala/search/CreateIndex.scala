package search

import org.apache.spark.{SparkConf, SparkContext}

object CreateIndex {
  def main(args: Array[String]): Unit = {

    val importFileName = "smallTestSample.csv"
    val exportFileName = "invertedIndex.bin"

    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val t1 = System.nanoTime()
    val indexMachine = new IndexMachine()
    indexMachine.createIndex(importFileName, exportFileName, sc)

    val t2 = System.nanoTime()
    //Index wird gespeichert
    println("Index in" + (t2 - t1) / 1e+6 / 1000 + " Sek.")

  }
}
