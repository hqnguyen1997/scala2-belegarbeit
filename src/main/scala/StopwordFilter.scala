import scala.io.Codec

object StopwordFilter {
def filter(dirty:Array[(String)],lang:String):Array[(String)]={

  val wordsSource=getClass.getResourceAsStream( lang match {
    case "DE" =>"stop-words-german.txt"
    case "ENG" =>"stopwords-eng.txt"
    case _ =>"empty.txt"
  })
  val stopwords = scala.io.Source.fromInputStream( wordsSource )(Codec("ISO-8859-1")).getLines()

  dirty.filterNot(stopwords contains  _)
}
}
