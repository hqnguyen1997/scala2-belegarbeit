package search

import utils.{LangCode, LanguageDetector}

import scala.io.Codec

object StopwordFilter {
  def filter(dirty: Array[(String)], lang: String): Array[(String)] = {
    //Länderkürzel -> häufigsten Worte dieser Sprache
    val wordsSource = getClass.getResourceAsStream(lang match {
      case LangCode.GERMAN => "/stop-words-german.txt"
      case LangCode.ENGLISH => "/stopwords-eng.txt"
      case _ => "/empty.txt"
    })
    val stopwords = scala.io.Source.fromInputStream(wordsSource)(Codec("ISO-8859-1")).getLines()
    //Stopwörter/häufigsten Worte dieser Sprache  werden entfernt
    dirty.filterNot(stopwords contains _)
  }
}
