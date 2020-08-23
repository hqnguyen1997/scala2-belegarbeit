package minhash

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class LshIndex(shingleLength: Int, signatureLength: Int, seed: Int, numBuckets: Int, output: String, sc: SparkContext) extends Serializable {

  /* Define how many rows should be in a bucket */
  val rows = signatureLength / numBuckets

  val rowsBc = sc.broadcast(rows)
  val shingleLengthBc = sc.broadcast(shingleLength)
  val signatureLengthBc = sc.broadcast(signatureLength)
  val seedBc = sc.broadcast(seed)


  def createIndex(data: RDD[(String, String)]) = {
    val minhashSignature = generateMinhashSignature(data)
    val candidatePairs = groupCandidatePairs(minhashSignature)
    val matchingsPairs = getMatchingPair(candidatePairs)
    val lshIndex = matchingsPairs.flatMap { case (doc1, doc2) =>
      Set((doc1, doc2), (doc2, doc1))
    }.aggregateByKey(collection.mutable.Set.empty[String])((s, v) => s += v, (h1, h2) => h1 ++= h2).map { case (key, cluster) =>
      cluster += key
    }.distinct()

    // Save Index in text file
    lshIndex.map(cluster => cluster.mkString(" ")).saveAsTextFile(output)
  }

  /**
   * Generate Minhash Signature matrix
   *
   * @param data RDD(url, text)
   */
  def generateMinhashSignature(data: RDD[(String, String)]): RDD[(String, Array[Int])] = {
    data.map {
      case (id, text) =>
        // Some data contains no hashable content, it would throw exception
        try {
          val minHash = new MinHash(text, signatureLengthBc.value, shingleLengthBc.value, seedBc.value)
          (id, minHash.generateMinHashSignature())
        } catch {
          case e: Exception => null
        }
    }.filter(el => el != null)
  }

  /**
   * Group all candidate duplicated pairs into bucket
   */
  def groupCandidatePairs(minhashSignatures: RDD[(String, Array[Int])]): RDD[Set[(String, Array[Int])]] = {
    val buckets = minhashSignatures.flatMap { case (id, signature) =>
      signature.grouped(rowsBc.value).zipWithIndex.map { case (band, bandIndex) =>
        ((bandIndex, band.toList.hashCode), (id, signature))
      }
    }.aggregateByKey(collection.mutable.Iterable.empty[(String, Array[Int])])((s, v) => s ++ Iterable(v), (i1, i2) => i1 ++ i2)

    buckets.flatMap { case ((bandIndex, bucketId), cluster) =>
      cluster.flatMap(doc1 => cluster.map(doc2 => Set(doc1, doc2)))
    }.distinct().cache()
  }

  def getMatchingPair(candidatePairsRDD: RDD[Set[(String, Array[Int])]]): RDD[(String, String)] = {
    candidatePairsRDD.map { pair =>
      if (pair.size == 1) {
        (pair, 1.0D)
      } else {
        (pair, MinHash.minhashSimilarity(pair.head._2, pair.tail.head._2))
      }
    }.filter { case (pair, score) =>
      score > 0.8D
    }.map { case (pair, score) =>
      if (pair.size == 1) {
        (pair.head._1, pair.head._1)
      } else {
        (pair.head._1, pair.tail.head._1)
      }
    }
  }

}

object LshIndex {
  def main(args: Array[String]): Unit = {
    val indexDataSource = "smallTestSample.csv"
    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    val sc = new SparkContext(conf)

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

    val lshIndex = new LshIndex(5, 100, 5, 20, "lshindex", sc)
    lshIndex.createIndex(data)
    sc.stop()
  }
}