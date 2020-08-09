package search

import stemmers.{EnglishStemmer, GermanStemmer}
import utils.{LanguageDetector, Tokenizer}

class SearchMachine(index:InvertedIndex) {

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
    val tokenTF:Map[String,Map[String,Int]]=stemmedTokens .map(token=>(token,(index.getIndex()(token)))).toMap
    //die  Term Frequency, Document Frequency wird ermittelt
    val tokenTFDF=tokenTF.map(rec=>(rec._1,rec._2,rec._2.size))
    // die Corpusgröße wird ermittelt
    val tokenTFDFCorupusSize=tokenTFDF.map(rec=>(rec._2,rec._3, index.getSize()))
    //tf idf wird berechnet
    val tf_idf=tokenTFDFCorupusSize.map(rec=>(rec._1,Math.log10(rec._3.toDouble/rec._2.toDouble)))


    tf_idf.flatMap(rec=>rec._1.map { case (k, v) => (k, v * rec._2) }).toMap
  }
}
