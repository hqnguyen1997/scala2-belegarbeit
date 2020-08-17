package minhash

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, HashMap}

class LshIndex(bandSize: Int = 4) {

  private val index = new HashMap[String, ArrayBuffer[String]]

  def insert(key: String, minhash: Minhash): LshIndex = {
    val hashbands = this.getHashbands(minhash)

    @tailrec
    def helper(i: Int, max: Int): LshIndex = {

      if (i == hashbands.length)
        this
      else {
        val band = hashbands(i)
        if (!index.contains(band)) {
          index(band) = ArrayBuffer[String](key)
        } else {
          index(band) += key
        }
        helper(i + 1, max)
      }
    }

    helper(0, hashbands.length)
  }


  def query(minhash: Minhash): Set[String] = {
    @tailrec
    def helper(hashbands: ArrayBuffer[String], minhash: Minhash, matches: Set[String] = Set.empty, i: Int = 0, j: Int = 0): Set[String] = {

      if (i == hashbands.length - 1)
        matches
      else {
        val band = hashbands(i)

        if (j == index(band).length - 1) {
          helper(hashbands, minhash, matches, i + 1, 0)
        } else {
          helper(hashbands, minhash, matches + index(band)(j): Set[String], i, j + 1)
        }
      }
    }

    helper(this.getHashbands(minhash), minhash)

  }

  def getHashbands(minhash: Minhash): ArrayBuffer[String] = {

    if (!minhash.hashbandsStr.isEmpty) minhash.hashbandsStr
    for (i <- 0 to (minhash.hashvalues.length / this.bandSize)) {
      val start = i * this.bandSize
      val end = start + this.bandSize
      val band = minhash.hashvalues.slice(start, end)
      minhash.hashbandsStr += band.mkString(".")
    }
    minhash.hashbandsStr
  }

}

object LshIndex {
  def main(args: Array[String]): Unit = {

    val s1: Array[String] = Array("minhash", "is", "a", "probabilistic", "data", "structure", "for",
      "estimating", "the", "similarity", "between", "datasets")
    val s2: Array[String] = Array("minhash", "is", "a", "probability", "data", "structure", "for",
      "estimating", "the", "similarity", "between", "documents")
    val s3: Array[String] = Array("cats", "are", "tall", "and", "have", "been", "known", "to", "sing", "quite", "loudly")
    // generate a hash for each list of words
    val m1 = new Minhash().inithashvalues().initPermutations
    val m2 = new Minhash().inithashvalues().initPermutations
    val m3 = new Minhash().inithashvalues().initPermutations

    // update each hash
    s1.map(w => m1.update(w))
    s2.map(w => m2.update(w))
    s3.map(w => m3.update(w))


    // add each document to a Locality Sensitive Hashing index
    val index = new LshIndex()

    val newIndex = index.insert("m1", m1).insert("m2", m2).insert("m3", m3)

    // query for documents that appear similar to a query document
    val matches = newIndex.query(m1)
    matches.foreach(w => println(w))
  }
}
