import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
class LshIndex {
private  val bandSize = 4
private  var index = new HashMap[String, ArrayBuffer[String]]
  def insert(key:String,minhash:Minhash):Unit={
    var hashbands = this.getHashbands(minhash)
    for (i <- 0  until  hashbands.length) {
      var band = hashbands(i)

      if (!index.contains(band)) {
        index(band)=ArrayBuffer[String](key)
      } else {
        index(band)+=key
      }
    }
  }


  def query (minhash:Minhash):Set[String]= {
    var matches = Set.empty[String]
    var hashbands = this.getHashbands(minhash)
    for ( i <- 0 until hashbands.length) {
      var band = hashbands(i)
      for ( j <- 0  until index(band).length) {
        matches+=index(band)(j)
      }
    }
    matches
  }

 def getHashbands(minhash:Minhash): ArrayBuffer[String]={

    if (!minhash.hashbandsStr.isEmpty)  minhash.hashbandsStr
    for ( i <- 0 to  (minhash.hashvalues.length / this.bandSize) ) {
      var start = i * this.bandSize
      var end = start + this.bandSize
      var band = minhash.hashvalues.slice(start, end)
      minhash.hashbandsStr+=band.mkString(".")
    }
     minhash.hashbandsStr

}




}
object LshIndex{
  def main(args: Array[String]): Unit = {

    var s1:Array[String] = Array("minhash", "is", "a", "probabilistic", "data", "structure", "for",
      "estimating", "the", "similarity", "between", "datasets")
    var s2:Array[String] = Array("minhash", "is", "a", "probability", "data", "structure", "for",
      "estimating", "the", "similarity", "between", "documents")
    var s3:Array[String] = Array("cats", "are", "tall", "and", "have", "been","known", "to", "sing", "quite", "loudly")
    // generate a hash for each list of words
    var m1 = new Minhash()
    var m2 = new Minhash()
    var m3 = new Minhash()

    // update each hash
    s1.map(w=> m1.update(w) )
    s2.map(w=> m2.update(w) )
    s3.map(w=> m3.update(w) )


    // add each document to a Locality Sensitive Hashing index
    var index = new LshIndex()
    index.insert("m1", m1)
    index.insert("m2", m2)
    index.insert("m3", m3)

    // query for documents that appear similar to a query document
    var matches = index.query(m1);
    matches.foreach(w=>println(w))
  }
  }
