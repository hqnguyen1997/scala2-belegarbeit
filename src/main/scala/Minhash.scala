import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}

class Minhash(var seed:Int=1,
              var hashbands:ArrayBuffer[Long]=ArrayBuffer.empty,
              var hashbandsStr:ArrayBuffer[String]=ArrayBuffer.empty,
              var hashvalues:ArrayBuffer[Long]=ArrayBuffer.empty,
              var permA:ArrayBuffer[Int]=ArrayBuffer.empty,
              var permB:ArrayBuffer[Int]=ArrayBuffer.empty
             ) {

  val numPerm:Int=128

  // prime is the smallest prime larger than the largest
  // possible hash value (max hash = 32 bit int)
  private val prime = 4294967311l
  //Math.pow(2, 32) - 1
  private val maxHash = 4294967295l



  def inithashvalues(): Minhash = {
    @tailrec
    def helper(i:Int=0,minhash: Minhash=this): Minhash = {
      if (i == (minhash.numPerm - 1))
        minhash
      else
        helper(i + 1, new Minhash(minhash.seed, minhash.hashbands, minhash.hashbandsStr, minhash.hashvalues ++ ArrayBuffer(minhash.maxHash), minhash.permA, minhash.permB))
    }
    helper()
  }

  // initialize the permutation functions for a & b
  // don't reuse any integers when making the functions
  def initPermutations: Minhash = {

    def helper(i: Int=0):Minhash={
      var used: Map[Int, Boolean] = Map.empty
      if(i>1)
        this
      else
        {


          var perms = scala.collection.mutable.ArrayBuffer.empty[Int]

          for (j <- 0 to this.numPerm) {
            var int: Int = this.randInt()
            while (used.exists(_ == int)) {
              int = this.randInt()
            }
            perms += int

            used = used ++ Map(int -> true)
          }

          if (i == 0)
            this.permA = perms
          else
            this.permB = perms


          helper(i+1)
        }



    }

    helper()

  }

  def hash(str: String): Long = {

    if (str.length == 0)
       this.maxHash


    def helper(hash:Int=0,i:Int=0):Long={
      if(i == (str.length ))
        hash+this.maxHash
      else {
        val char = str.charAt(i)
        val hash1 = ((hash << 5) - hash) + char
        val hash2 = hash1 & hash1 // convert to a 32bit integer
        helper(hash2,i+1)
      }
    }
    helper()

  }

  def update(str: String): Unit = {

    for (i <- 0 until this.hashvalues.length) {

      var a: Long = this.permA(i)
      var b: Long = this.permB(i)

      var hash = (a * this.hash(str) + b) % this.prime
      if (hash < this.hashvalues(i)) {
        this.hashvalues(i) = hash
      }
    }
  }

  // estimate the jaccard similarity to another minhash

  def jaccard(other: Minhash): Double = {
    if (this.hashvalues.length != other.hashvalues.length) {
      println("ashvalue counts differ")
    } else if (this.seed != other.seed) {
      println("seed values differ")
    }


    @tailrec
    def helper(shared:Int=0,i:Int=0):Int={
      if(i==this.hashvalues.length)
        shared
      else

      if (this.hashvalues(i) == other.hashvalues(i))
        helper(shared+1,i+1)
      else
        helper(shared,i+1)

    }
    val shared=helper().toDouble
    shared / this.hashvalues.length.toDouble
  }


  def randInt(): Int = {
    this.seed = this.seed + 1

    var x = Math.sin(this.seed) * this.maxHash;
    Math.floor((x - Math.floor(x)) * this.maxHash).toInt
  }




}

object Minhash {
  def main(args: Array[String]) {

    var s1: Array[String] = Array("minhash", "is", "a", "probabilistic", "data", "structure", "for",
      "estimating", "the", "similarity", "between", "datasets")
    var s2: Array[String] = Array("minhash", "is", "a", "probability", "data", "structure", "for",
      "estimating", "the", "similarity", "between", "documents")

    // create a hash for each set of words to compare
    var m1 = new Minhash().inithashvalues().initPermutations
    var m2 = new Minhash().inithashvalues().initPermutations

    // update each hash
    s1.map(w => m1.update(w))
    s2.map(w => m2.update(w))
    // estimate the jaccard similarity between two minhashes
    println(m1.jaccard(m2))
  }

}
