package utils

import scala.io.{Codec, Source}

object LanguageDetector {

  val topLanguages = Map(
    LangCode.GERMAN -> "top1000de.txt",
    LangCode.ENGLISH -> "google-10000-english.txt",
    LangCode.POLISH -> "top1000pl.txt",
    LangCode.SPANISH -> "top1000esp.txt",
    LangCode.RUSSIAN -> "top1000ru.txt",
    LangCode.PORTUGUESE -> "top1000prt.txt",
    LangCode.GREEK -> "top1000grc.txt"
  )

  /**
   * Caculate score of tokens in each language
   * return language name (Standard "ISO 639-1")
   *
   * @param tokens
   * @return
   */
  def detect(tokens: Array[String]): String = {
    //Alle tokens werden nur einmalig betrachtet
    val tokensDistinct = tokens.toSet
    //ein Scoring wird ermittelt, wie oft wird ein token, in einer Stopwortliste gefunden
    val scoring = topLanguages.map(t => {
      val wordsSource = getClass.getResourceAsStream("/" + t._2)
      val wordsSet = Source.fromInputStream(wordsSource)(Codec("ISO-8859-1")).getLines().toSet
      val score = wordsSet.intersect(tokensDistinct).size
      (t._1, score)
      //Eine Extra Scoring wird addiert damit auch unbekannte Token ermittelt werden können
    }) ++ Map("UNKNOWN" -> (1 + tokensDistinct.size / 10))
    // detected language, die Sprache mit höchsten Score wird vermutet
    scoring.maxBy { case (key: String, value: Int) => value }._1
  }
}
