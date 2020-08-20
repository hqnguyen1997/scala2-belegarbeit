package search

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import stemmers.{EnglishStemmer, GermanStemmer}
import utils.{LangCode, LanguageDetector, Tokenizer}

class IndexMachine {

  def createIndex(dataSource: String, indexOutput: String, sc: SparkContext): InvertedIndex = {
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

    val invertedIndex = new InvertedIndex(preparedData)

    this.saveIndex(invertedIndex, indexOutput)

    println("Datei gespeichert:" + indexOutput)
    println("Anzahl der Dokumente:" + invertedIndex.getSize())

    invertedIndex
  }

  def saveIndex(index: InvertedIndex, indexOutput: String): Unit = {
    val oos = new ObjectOutputStream(new FileOutputStream(indexOutput))
    try {
      oos.writeObject(index)
    } finally {
      oos.close()
    }
  }

  def loadIndex(indexSrc: String): InvertedIndex = {
    val ois = new ObjectInputStream(new FileInputStream(indexSrc))
    try {
      ois.readObject.asInstanceOf[InvertedIndex]
    } finally {
      ois.close()
    }
  }
}
