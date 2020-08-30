import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import minhash.LshIndex
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import search.{InvertedIndex, SearchMachine}

import scala.io.StdIn


object WebServer {
  val indexDataSource = "smallTestSample.csv"
  val indexOutput = "invertedIndex"
  val lshIndexOutput = "lshindex"

  def main(args: Array[String]) {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val searchMachine = initSearchServer(indexDataSource, indexOutput, sc)
    val lshIndex = initLshIndex(indexDataSource, lshIndexOutput, sc)

    val route = concat(
      pathPrefix("api") {
        path("search") {
          get {
            parameter("query".as[String],"language".as[String]) { (query,language) =>
              if (query.contains("url:::")) {
                val params = query.split(":::")
                val result = searchMachine.searchUrl(params(1))
                complete(HttpEntity(ContentTypes.`application/json`, "{\"" + result + "\": 1 }"))
              } else {
                val result = searchMachine.search(query,language)
                if (result.size == "") {
                  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "{}"))
                } else {
                  val jsonResult = scala.util.parsing.json.JSONObject(result)
                  complete(HttpEntity(ContentTypes.`application/json`, jsonResult.toString()))
                }
              }
            }
          }
        }
      },
      pathPrefix("api") {
        path("duplicate") {
          get {
            parameter("url".as[String]) { url =>
              val result = findDuplicate(url, lshIndex)
              if(result.count()==0){
                val jsonResult = scala.util.parsing.json.JSONObject(Map("result" -> ""))
                complete(HttpEntity(ContentTypes.`application/json`, jsonResult.toString()))
              }else{
              val arrayResult = result.first.deep.mkString(",")
              val jsonResult = scala.util.parsing.json.JSONObject(Map("result" -> arrayResult))
              complete(HttpEntity(ContentTypes.`application/json`, jsonResult.toString()))
              }
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

  def initSearchServer(indexDataSource: String, indexOutput: String, sc: SparkContext): SearchMachine = {
    println("Init Search Server")
    val invertedIndex = new InvertedIndex()

    try {
      invertedIndex.createIndex(indexDataSource, indexOutput, sc)
    } catch {
      case e: Exception => println("Already indexed Inverted Index")
    }

    val index = invertedIndex.loadIndex(indexOutput, sc)

    new SearchMachine(index)

  }

  def initLshIndex(indexDataSource: String, indexOutput: String, sc: SparkContext): RDD[Array[String]] = {
    println("Init LSH Index")
    val csvData = sc.textFile(indexDataSource)

    val data: RDD[(String, String)] = csvData.map(line => {
      // Read lines
      val splitted: Array[String] = line.split(";")
      // Get url
      if (splitted.size == 2) {
        (splitted(0), splitted(1).replaceAll("\\[[^\\)]*\\]", ""))
      } else {
        ("", "")
      }
    })

    val lshIndex = new LshIndex(5, 100, 5, 20, indexOutput, sc)

    try {
      lshIndex.createIndex(data)
    } catch {
      case e: Exception => println("Already indexed LSH Index")
    }

    val index = sc.textFile(indexOutput + "/part-*").map(line => line.split(" "))

    index
  }

  def findDuplicate(url: String, lshIndex: RDD[Array[String]]): RDD[Array[String]] = {
    lshIndex.filter(item => item.contains(url))
  }
}
