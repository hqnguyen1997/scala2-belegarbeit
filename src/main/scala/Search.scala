import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object Search {
  def main(args: Array[String]): Unit = {
    val ii = new InvertedIndex(Map.empty, 0).loadFromFile("invertedIndex.bin")
    val results = ii.search("songs lyrics").toSeq.sortWith(_._2>_._2 ).map(_._1)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val writer = mapper.writerWithDefaultPrettyPrinter
    val json = writer.writeValueAsString(results)
    println(json)

  }
}
