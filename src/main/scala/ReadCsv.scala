import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ReadCsv {
def readFile(file:String,sc:SparkContext):RDD[(String,String)]={
  val filePath = getClass.getResource("/"+file).getFile
  val rddFromFile = sc.textFile(filePath)
  val rdd = rddFromFile.flatMap(f=>{
    f.split("\r\n")
  })
    rdd.map(record=>(record.split(";")(0),record.split(";")(1)))
}


}
