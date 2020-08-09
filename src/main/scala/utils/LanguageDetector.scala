package utils

import scala.io.{Codec, Source}

object LanguageDetector {
  val LANGCODE_GERMAN="de"
  val LANGCODE_ENGLISH="en"
  val LANGCODE_POLISH="pl"
  val LANGCODE_SPANISH="es"
  val LANGCODE_RUSSIAN="ru"
  val LANGCODE_PORTUGUESE="pt"
  val LANGCODE_GREEK="grc"

  val topLanguages = Map(
    this.LANGCODE_GERMAN -> "top1000de.txt",
    this.LANGCODE_ENGLISH -> "google-10000-english.txt",
    this.LANGCODE_POLISH -> "top1000pl.txt",
    this.LANGCODE_SPANISH -> "top1000esp.txt",
    this.LANGCODE_RUSSIAN -> "top1000ru.txt",
    this.LANGCODE_PORTUGUESE -> "top1000prt.txt",
    this.LANGCODE_GREEK -> "top1000grc.txt"
  )

  /**
   * Caculate score of tokens in each language
   * return language name (Standard "ISO 639-1")
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

    }) ++      Map("UNKNOWN" -> (1 + tokensDistinct.size / 10)) //Eine Extra Scoring wird addiert damit auch unbekannte Token ermittelt werden können
    // detected language, die Sprache mit höchsten Score wird vermutet
    scoring.maxBy { case (key: String, value: Int) => value }._1
  }

}
