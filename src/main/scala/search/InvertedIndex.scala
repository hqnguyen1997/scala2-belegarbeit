package search

import org.apache.spark.rdd.RDD
import stemmers.{EnglishStemmer, GermanStemmer}
import utils.{LanguageDetector, Tokenizer}

class InvertedIndex(data: RDD[(String, Map[String, Int])]) extends Serializable {

  val index: Map[String, Map[String, Int]] =
    data
      .flatMap(tuple => tuple._2.map(x => (x._1, (tuple._1, x._2)))).mapValues(Map(_)).reduceByKey(_ ++ _)
      .collect().toMap

  val size = data.count().toInt

  def getSize(): Int = size

  def getIndex(): Map[String, Map[String, Int]] = index

}
