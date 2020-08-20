package minhash

import org.scalatest.FunSuite

class MinHash2Test extends FunSuite {

  test("testGenerateShingles") {
    val text = "This is a very helpful test text"
    val shingleLength: Int = 3

    val minHash = new MinHash2(text)

    val shingles = minHash.generateShingles()

    shingles.foreach(shingle => {
      assert(shingle.split(" ").length == shingleLength)
    })
  }

  test("Generate Shingles, dot and commands should out") {
    val text = "This is a very, helpful test text. But just a test"
    val shingleLength: Int = 3

    val minHash = new MinHash2(text)

    val shingles = minHash.generateShingles()

    shingles.foreach(shingle => {
      assert(shingle.split(" ").length == shingleLength)
      assert(!shingle.contains("."))
      assert(!shingle.contains(","))
    })
  }

  test("Generate Shingles with other shingle length") {
    val text = "This is a very, helpful test text. But just a test"
    val shingleLength: Int = 5

    val minHash = new MinHash2(text, 100, shingleLength, 5)

    val shingles = minHash.generateShingles()

    shingles.foreach(shingle => {
      assert(shingle.split(" ").length == shingleLength)
    })
  }

  test("Minhash signature") {
    val text = "This is a very, helpful test text. But just a test"

    val minHash = new MinHash2(text)

    val sigs = minHash.generateMinHashSignature()
    assert(sigs != null)
  }

  test("Minhash signature length should be equal as given (default 100)") {
    val text = "This is a very, helpful test text. But just a test"

    val minHash = new MinHash2(text)

    val sigs = minHash.generateMinHashSignature()
    assert(sigs.length == 100)
  }

  test("Minhash signature length should be equal as given") {
    val text = "This is a very, helpful test text. But just a test"

    val minHash = new MinHash2(text, 200, 3, 5)

    val sigs = minHash.generateMinHashSignature()
    assert(sigs.length == 200)
  }

  test("Calculate Similarity of same string") {
    val text = "Some test text, compare with itself"
    val minHash1 = new MinHash2(text)
    val minHash2 = new MinHash2(text)

    assert(MinHash2.minhashSimilarity(minHash1.generateMinHashSignature(), minHash2.generateMinHashSignature()) == 1)
  }

  test("Calculate Similarity of very difference strings") {
    val text1 = "Some test text, compare with other"
    val text2 = "Einfach nicht gleich"
    val minHash1 = new MinHash2(text1)
    val minHash2 = new MinHash2(text2)
    assert(MinHash2.minhashSimilarity(minHash1.generateMinHashSignature(), minHash2.generateMinHashSignature()) < 0.1)
  }

  test("Calculate similarity of difference strings, around 50% similar") {
    val text1 = "Some test text, compare with other"
    val text2 = "Some test text, compare"
    val minHash1 = new MinHash2(text1)
    val minHash2 = new MinHash2(text2)
    println(minHash1.generateMinHashSignature().deep.mkString(","))
    assert(MinHash2.minhashSimilarity(minHash1.generateMinHashSignature(), minHash2.generateMinHashSignature()) > 0.4
      && MinHash2.minhashSimilarity(minHash1.generateMinHashSignature(), minHash2.generateMinHashSignature()) < 0.6)
  }
}
