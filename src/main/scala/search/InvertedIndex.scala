package search

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.rdd.RDD
import stemmers.{EnglishStemmer, GermanStemmer}
import utils.{LanguageDetector, Tokenizer}

class InvertedIndex(data:RDD[(String, Map[String, Int])]) extends Serializable {

  val index:Map[String, Map[String,Int]] =
    data
      .flatMap(tuple => tuple._2.map(x => (x._1, (tuple._1, x._2)))).mapValues(Map(_)).reduceByKey(_ ++ _)
      .collect().toMap

  val size = data.count().toInt

  def getSize():Int = size
  def getIndex():Map[String, Map[String,Int]] = index

//  def saveIndex(file:String):Unit={
//    val oos = {
//      new ObjectOutputStream(new FileOutputStream(file))
//    }
//    try {
//      oos.writeObject((index,getSize()))
//    } finally {
//      oos.close()
//    }
//  }
//
//  def loadIndex(file:String): InvertedIndex ={
//
//    val ois = new ObjectInputStream(new FileInputStream(file))
//    try {
//      val rawIndex = ois.readObject.asInstanceOf[(Map[String, Map[String,Int]],Int)]
//      new InvertedIndex(rawIndex._1,rawIndex._2)
//    } finally {
//      ois.close()
//    }
//  }

  def search(query:String):Map[String,Double]={
    //die suchanfrage wird tokenisiert
    val tokens=Tokenizer.tokenize(query)
    //es wird ermittelt um welche Sprache es sich handelt
    val detectedlanguage=LanguageDetector.detect(tokens)
    //abhängig von der Sprache werden die Stopworte entfernt
    val filterdStopwords=StopwordFilter.filter(tokens,detectedlanguage)
    //abhängig von der Sprache werden die passenden Stemmer benutzt
    val stemmedTokens= if(detectedlanguage=="DE")filterdStopwords.map(GermanStemmer.stem) else filterdStopwords.map(EnglishStemmer.stem)

    //die  Term Frequency wird ermittelt
    val tokenTF:Map[String,Map[String,Int]]=stemmedTokens .map(token=>(token,(index(token)))).toMap
    //die  Term Frequency, Document Frequency wird ermittelt
    val tokenTFDF=tokenTF.map(rec=>(rec._1,rec._2,rec._2.size))
    // die Corpusgröße wird ermittelt
    val tokenTFDFCorupusSize=tokenTFDF.map(rec=>(rec._2,rec._3,getSize()))
    //tf idf wird berechnet
    val tf_idf=tokenTFDFCorupusSize.map(rec=>(rec._1,Math.log10(rec._3.toDouble/rec._2.toDouble)))


    tf_idf.flatMap(rec=>rec._1.map { case (k, v) => (k, v * rec._2) }).toMap
  }
}
