//import akka.actor.ActorSystem
//
//import scala.io.StdIn
//
//
//object WebServer {
//  def main(args: Array[String]) {
//
//    implicit val system = ActorSystem("my-system")
//    implicit val materializer = ActorMaterializer()
//    // needed for the future flatMap/onComplete in the end
//    implicit val executionContext = system.dispatcher
//
//    val invertedIndex = new InvertedIndex(Map.empty,0).loadFromFile("invertedIndex.bin")
//
//    val route =
//      concat(
//      path("api") {
//      get{
//        parameter("query".as[String]) { (query) =>
//          val result=invertedIndex.search(query)
//          if(result.size=="") {
//            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,"{}" ))
//          }else {
//
//          val jsonResult=scala.util.parsing.json.JSONObject(result)
//
//          complete(HttpEntity(ContentTypes.`application/json`,jsonResult.toString() ))
//          }
//        }
//      }
//      },      path("") {
//        get{
//
//              complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,scala.io.Source.fromFile("searchengine.html").mkString ))
//
//
//        }
//        }
//
//      )
//
//    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
//
//    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
//    StdIn.readLine() // let it run until user presses return
//    bindingFuture
//      .flatMap(_.unbind()) // trigger unbinding from the port
//      .onComplete(_ => system.terminate()) // and shutdown when done
//  }
//}
