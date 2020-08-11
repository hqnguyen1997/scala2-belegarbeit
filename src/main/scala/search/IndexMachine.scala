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
      //Zeilen lesen
      val splitted: Array[String] = line.split(";")
      //aus der Zeile Vorkommenshäufigkeit und Url entnehmen
      if (splitted.size == 2) (splitted(0), splitted(1)) else ("", "")
    })

    val preparedData: RDD[(String, Map[String, Int])] =
      rows
        // (url, token)
        .map(rec => (rec._1, Tokenizer.tokenize(rec._2)))
        //url , tokens , language code
        .map(rec => (rec._1, rec._2, LanguageDetector.detect(rec._2)))
        //nur unsere bevorzugten sprachen werden herausgenommen
        .filter(preferredLanguages contains _._3)
        //Stopworte werden gefiltert
        .map(rec => (rec._1, StopwordFilter.filter(rec._2, rec._3), rec._3))
        //url, stemmedTokens
        .map(rec => (
          rec._1,
          if (rec._3 == LangCode.GERMAN) rec._2.map(word => GermanStemmer.stem(word))
          else rec._2.map(word => EnglishStemmer.stem(word))
        ))
        //term frequency wird ermittelt
        .map(rec => (rec._1 -> Tf.tf(rec._2)))

    val invertedIndex = new InvertedIndex(preparedData)
    // Index wird gespeichert
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
