package search

import org.apache.spark.rdd.RDD
import stemmers.{EnglishStemmer, GermanStemmer}
import utils.{LanguageDetector, Tokenizer}

class SearchMachine(index: RDD[(String,Iterable[Map[String, Int]])]) {

  /**
   *
   * @param query
   * @return url and score
   */
  val corpusSize:Int=index.count().toInt

  def search(query: String,language: String): Map[String, Double] = {
    val limit=50
    // Tokenize query
    val tokens = Tokenizer.tokenize(query)
    // Filter stop words
    val filterdStopwords = StopwordFilter.filter(tokens, language)
    // Using stemmer
    val stemmedTokens = if (language == "DE") filterdStopwords.map(GermanStemmer.stem) else filterdStopwords.map(EnglishStemmer.stem)

    // Calculate term frequency

    val filteredIndex:RDD[(String,Iterable[Map[String, Int]])]=index.filter(token=>stemmedTokens.contains(token._1))
    val tokenTF: RDD[(String, Iterable[Map[String, Int]])] =  filteredIndex

    // Calculate term frequency, and document frequency
    val tokenTFDF = tokenTF.map(rec => (rec._1, rec._2, rec._2.size))



    val corpusCount=this.corpusSize
    val tokenTFDFCorupusSize = tokenTFDF.map(rec => (rec._2, rec._3,corpusCount))

    // Calculate tf
    val tf = tokenTFDFCorupusSize.map(rec => (rec._1, Math.log10(rec._3.toDouble / rec._2.toDouble)))

    //calculate idf

    val tf_idf= tf.flatMap(rec=>rec._1.flatMap(rec2=>rec2.map(rec3=>(rec3._1,rec3._2*rec._2))))



    tf_idf.takeOrdered(limit)(Ordering[Double].reverse.on(x=>x._2)).toMap



  }

  def searchUrl(url: String): String = {
    if (index.filter(_._1 == url) != null) url
    else ""
  }
}
