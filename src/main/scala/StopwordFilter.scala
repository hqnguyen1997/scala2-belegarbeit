import utils.LanguageDetector

import scala.io.Codec

object StopwordFilter {
def filter(dirty:Array[(String)],lang:String):Array[(String)]={
  //Länderkürzel -> häufigsten Worte dieser Sprache
  val wordsSource=getClass.getResourceAsStream( lang match {
    case LanguageDetector.LANGCODE_GERMAN =>"stop-words-german.txt"
    case LanguageDetector.LANGCODE_ENGLISH =>"stopwords-eng.txt"
    case _ =>"empty.txt"
  })
  val stopwords = scala.io.Source.fromInputStream( wordsSource )(Codec("ISO-8859-1")).getLines()
  //Stopwörter/häufigsten Worte dieser Sprache  werden entfernt
  dirty.filterNot(stopwords contains  _)
}
}
