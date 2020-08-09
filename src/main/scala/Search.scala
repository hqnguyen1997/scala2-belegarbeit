import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object Search {
  def main(args: Array[String]): Unit = {
    //es wird der Invertierte Index erstellt
    val ii = new InvertedIndex(Map.empty, 0).loadFromFile("invertedIndex.bin")
    //Der Suchbegriff wird  in dem ii gesucht und sortiert nach dem Tf * idf, die Vorkommenshäufigkeit wird  zum schluss entfernt
    val results = ii.search("songs lyrics").toSeq.sortWith(_._2>_._2 ).map(_._1)
    //das ergebnis wird in ein JSON umgewandelt und ausgegeben
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val writer = mapper.writerWithDefaultPrettyPrinter
    val json = writer.writeValueAsString(results)
    println(json)

  }
}
