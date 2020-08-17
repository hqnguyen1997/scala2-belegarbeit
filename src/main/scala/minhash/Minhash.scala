package minhash

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, Map}

class Minhash(var seed: Int = 1,
              var hashbands: ArrayBuffer[Long] = ArrayBuffer.empty,
              var hashbandsStr: ArrayBuffer[String] = ArrayBuffer.empty,
              var hashvalues: ArrayBuffer[Long] = ArrayBuffer.empty,
              var permA: ArrayBuffer[Int] = ArrayBuffer.empty,
              var permB: ArrayBuffer[Int] = ArrayBuffer.empty
             ) {

  val numPerm: Int = 128

  // prime is the smallest prime larger than the largest
  // possible hash value (max hash = 32 bit int)
  val prime = 4294967311L
  //Math.pow(2, 32) - 1
  val maxHash = 4294967295L

  def inithashvalues(): Minhash = {
    @tailrec
    def helper(i: Int = 0, minhash: Minhash = this): Minhash = {
      if (i == minhash.numPerm)
        minhash
      else
        helper(i + 1,
          new Minhash(minhash.seed,
            minhash.hashbands,
            minhash.hashbandsStr,
            minhash.hashvalues ++ ArrayBuffer(minhash.maxHash),
            minhash.permA,
            minhash.permB)
        )
    }

    helper()
  }

  // initialize the permutation functions for a & b
  // don't reuse any integers when making the functions
  def initPermutations: Minhash = {
    @tailrec
    def helper(i: Int = 0): Minhash = {

      if (i > 1)
        this
      else {
        def permHelper(i: Int = 0,
                       used: Map[Int, Boolean] = Map.empty,
                       perms: ArrayBuffer[Int] = ArrayBuffer.empty
                      ): ArrayBuffer[Int] = {

          if (i == this.numPerm)
            perms
          else {
            var int: Int = this.randInt()
            while (used.exists(_ == int)) {
              int = this.randInt()
            }
            permHelper(i + 1, used ++ Map(int -> true), perms ++ ArrayBuffer(int))
          }
        }

        if (i == 0)
          this.permA = permHelper()
        else
          this.permB = permHelper()

        helper(i + 1)
      }
    }

    helper()
  }

  def hash(str: String): Long = {

    if (str.length == 0)
      this.maxHash

    @tailrec
    def helper(hash: Int = 0, i: Int = 0): Long = {
      if (i == str.length)
        hash + this.maxHash
      else {
        val char = str.charAt(i)
        val hash1 = ((hash << 5) - hash) + char
        val hash2 = hash1 & hash1 // convert to a 32bit integer
        helper(hash2, i + 1)
      }
    }

    helper()
  }

  def update(str: String): Unit = {

    for (i <- 0 until this.hashvalues.length) {

      val a: Long = this.permA(i)
      val b: Long = this.permB(i)

      val hash = (a * this.hash(str) + b) % this.prime
      if (hash < this.hashvalues(i)) {
        this.hashvalues(i) = hash
      }
    }
  }

  // estimate the jaccard similarity to another minhash

  def jaccard(other: Minhash): Double = {
    if (this.hashvalues.length != other.hashvalues.length) {
      println("hash value counts differ")
    } else if (this.seed != other.seed) {
      println("seed values differ")
    }

    @tailrec
    def helper(shared: Int = 0, i: Int = 0): Int = {
      if (i == this.hashvalues.length)
        shared
      else if (this.hashvalues(i) == other.hashvalues(i))
        helper(shared + 1, i + 1)
      else
        helper(shared, i + 1)
    }

    val shared = helper().toDouble
    shared / this.hashvalues.length.toDouble
  }

  def randInt(): Int = {
    this.seed = this.seed + 1

    val x = Math.sin(this.seed) * this.maxHash;
    Math.floor((x - Math.floor(x)) * this.maxHash).toInt
  }
}

object Minhash {
  def main(args: Array[String]) {

    val s1: Array[String] = Array("minhash", "is", "a", "probabilistic", "data", "structure", "for",
      "estimating", "the", "similarity", "between", "datasets")
    val s2: Array[String] = Array("minhash", "is", "a", "probability", "data", "structure", "for",
      "estimating", "the", "similarity", "between", "documents")
    val s3: Array[String] = Array("cats", "are", "tall", "and", "have", "been",
      "known", "to", "sing", "quite", "loudly")

    // create a hash for each set of words to compare
    val m1 = new Minhash().inithashvalues().initPermutations
    val m2 = new Minhash().inithashvalues().initPermutations
    val m3 = new Minhash().inithashvalues().initPermutations

    // update each hash
    s1.map(w => m1.update(w))
    s2.map(w => m2.update(w))
    s3.map(w => m3.update(w))
    // estimate the jaccard similarity between two minhashes
    println(m2.jaccard(m3))
  }

}
