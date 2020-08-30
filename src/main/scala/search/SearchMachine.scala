package search

import org.apache.spark.rdd.RDD
import stemmers.{EnglishStemmer, GermanStemmer}
import utils.{LanguageDetector, Tokenizer}

class SearchMachine(index: RDD[(String, Map[String, Int])]) {

  /**
   *
   * @param query
   * @return url and score
   */
  def search(query: String,language: String): Map[String, Double] = {
    val limit=50
    // Tokenize query
    val tokens = Tokenizer.tokenize(query)
    // Filter stop words
    val filterdStopwords = StopwordFilter.filter(tokens, language)
    // Using stemmer
    val stemmedTokens = if (language == "DE") filterdStopwords.map(GermanStemmer.stem) else filterdStopwords.map(EnglishStemmer.stem)

    // Calculate term frequency
    val stemmedTokensRDD =index.sparkContext.parallelize(stemmedTokens).map((token)=>(token,0))


    val tokenTF: RDD[(String, Map[String, Int])] =  stemmedTokensRDD.join(index).map(x=>(x._1,x._2._2))

    // Calculate term frequency, and document frequency
    val tokenTFDF = tokenTF.map(rec => (rec._1, rec._2, rec._2.size))

    // Calculate Corpusgröße
    val indexCount=index.count().toInt
    val tokenTFDFCorupusSize = tokenTFDF.map(rec => (rec._2, rec._3,indexCount))

    // Calculate tf idf
    val tf_idf = tokenTFDFCorupusSize.map(rec => (rec._1, Math.log10(rec._3.toDouble / rec._2.toDouble)))
    //limit the response results
    tf_idf.flatMap(rec => rec._1.map { case (k, v) => (k, v * rec._2) }).sortBy(_._2,false).take(limit).toMap

  }

  def searchUrl(url: String): String = {
    if (index.filter(_._1 == url) != null) url
    else ""
  }
}
