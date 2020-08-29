package search

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import stemmers.{EnglishStemmer, GermanStemmer}
import utils.{LangCode, LanguageDetector, Tokenizer}

class InvertedIndex {

  def createIndex(dataSource: String, indexOutput: String, sc: SparkContext) = {
    val csvData = sc.textFile(dataSource)
    val preferredLanguages = List(LangCode.GERMAN, LangCode.ENGLISH)

    val rows: RDD[(String, String)] = csvData.map(line => {
      // Read lines
      val splitted: Array[String] = line.split(";")
      // Get url
      if (splitted.size == 2) {
        (splitted(0), splitted(1))
      } else {
        ("", "")
      }
    })

    /**
     * -- Tokenize text => (url, token)
     * -- Determine language => (url, token, language_code)
     * -- Filter records, which are in preferred language
     * -- Filter stop word
     * -- Stemming => (url, stemmed_token)
     * -- Caculate term frequency
     *
     */
    val preparedData: RDD[(String, Map[String, Int])] =
      rows
        .map(rec => (rec._1, Tokenizer.tokenize(rec._2)))
        .map(rec => (rec._1, rec._2, LanguageDetector.detect(rec._2)))
        .filter(preferredLanguages contains _._3)
        .map(rec => (rec._1, StopwordFilter.filter(rec._2, rec._3), rec._3))
        .map(rec => (
          rec._1,
          if (rec._3 == LangCode.GERMAN) rec._2.map(word => GermanStemmer.stem(word))
          else rec._2.map(word => EnglishStemmer.stem(word))
        ))
        .map(rec => (rec._1 -> Tf.tf(rec._2)))

    val index = preparedData
      .flatMap(tuple => tuple._2.map(x => (x._1, (tuple._1, x._2)))).mapValues(Map(_)).reduceByKey(_ ++ _)

    this.saveIndex(index, indexOutput)

    println("Index saved:" + indexOutput)
  }

  def saveIndex(index: RDD[(String, Map[String, Int])], indexOutput: String): Unit = {
    index.map(item => item._1 + "||" + item._2.mkString("==")).saveAsTextFile(indexOutput)
  }

  def loadIndex(indexSrc: String, sc: SparkContext): RDD[(String, Map[String, Int])] = {
    val dataSrc = sc.textFile(indexSrc + "/part-*")

    val index: RDD[(String, Map[String, Int])] = dataSrc.map(line => {
      val splittedLine = line.split("[||]").map(item => item.trim)
      val word = splittedLine(0)
      val splittedMapString = splittedLine(2).split("==")
      val resultMap = splittedMapString.foldLeft(Map[String, Int]())((base, item) => {
        val splittedItem = item.split("->")
        base.updated(splittedItem(0).trim, splittedItem(1).trim.toInt)
      })
      (word, resultMap)
    })

    index
  }
}
