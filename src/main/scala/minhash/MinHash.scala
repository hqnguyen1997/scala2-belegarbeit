package minhash

import scala.util.hashing.{MurmurHash3}

class MinHash(text: String, signatureLength: Int = 100, shingleLength: Int = 3, seed: Int = 5) {

  /**
   * Generate array of shingles
   * shingle is a group of words
   * number of words in group should be given in constructor
   * default is 3
   *
   * @return
   */
  def generateShingles(): Iterator[String] = {
    text.split("[\\p{Punct}\\p{Blank}\\s]{1,}").sliding(shingleLength).map(gram => gram.mkString(" "))
  }

  def generateMinHashSignature(): Array[Int] = {
    (seed to (seed + signatureLength - 1)).map { randomSeed =>
      generateShingles().map(shingle => MurmurHash3.stringHash(shingle, randomSeed)).min
    }.toArray
  }
}

object MinHash {
  def minhashSimilarity[A](item1: Array[A], item2: Array[A]): Double = {
    if (item1.length != item2.length) {
      throw new IllegalArgumentException("MinHashes must be equal length")
    }

    val agreeingRows = item1.zip(item2).map { case (val1, val2) =>
      if (val1 == val2) {
        1
      } else {
        0
      }
    }.sum

    agreeingRows.toDouble / item1.length.toDouble
  }
}
