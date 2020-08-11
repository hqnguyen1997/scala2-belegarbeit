import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import org.apache.spark.{SparkConf, SparkContext}
import search.{IndexMachine, InvertedIndex, SearchMachine}

import scala.io.StdIn


object WebServer {
  val indexDataSource = "smallTestSample.csv"
  val indexSrc = "invertedIndex.bin"

  def main(args: Array[String]) {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val searchMachine = initSearchServer(indexDataSource, indexSrc, sc)

    val route = concat(
      path("api") {
        get {
          parameter("query".as[String]) { query =>
            val result = searchMachine.search(query)
            if (result.size == "") {
              complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "{}"))
            } else {
              val jsonResult = scala.util.parsing.json.JSONObject(result)
              complete(HttpEntity(ContentTypes.`application/json`, jsonResult.toString()))
            }
          }
        }
      },
      path("") {
        get {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, scala.io.Source.fromFile("searchengine.html").mkString))
        }
      }
    )

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }

  def initSearchServer(indexDataSource: String, indexSrc: String, sc: SparkContext): SearchMachine = {
    try {
      val invertedIndex = new IndexMachine().loadIndex("invertedIndex.bin")
      println("Ein Index ist vorhanden. Laden von Index erfolgreich")
      new SearchMachine(invertedIndex)
    } catch {
      case e: Exception => {
        println("Ein Index ist nicht vorhanden. Laden von Index wird jetztt ausgeführt")
        val t1 = System.nanoTime()

        val invertedIndex = new IndexMachine().createIndex(indexDataSource, indexSrc, sc)

        val t2 = System.nanoTime()
        //Index wird gespeichert
        println("Index generiert in: " + (t2 - t1) / 1e+6 / 1000 + " Sek.")

        new SearchMachine(invertedIndex)
      }
    }
  }
}
