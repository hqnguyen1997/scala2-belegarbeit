import org.apache.spark.sql.{SparkSession, types}
import java.sql.DriverManager

object ReadSql {

  //SQL kommt nicht mehr zum einsatz!!
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Belegarbeit")
      .getOrCreate();

    val url = "jdbc:mysql://localhost:3306/geniuscom?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&"
    val driver = "com.mysql.jdbc.Driver"
    Class.forName(driver)
    val username = "root"
    val password = ""
    var connection =  DriverManager.getConnection(url+"user="+username+"&password="+password)
    try {
      Class.forName(driver)

      val statement = connection.createStatement()
      val rs = statement.executeQuery("SELECT content,url as docNr FROM responses LIMIT 100")
      while (rs.next) {
        val docNr = rs.getString("docNr")
        println(docNr)
        val content = rs.getString("content")
        println(content)
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    connection.close


  }
}
