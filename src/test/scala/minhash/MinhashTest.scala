package minhash

import org.scalatest.FunSuite

class MinhashTest extends FunSuite {

  /*
  Test string
   */
  val s1: Array[String] = Array("minhash", "is", "a", "probabilistic", "data", "structure", "for",
    "estimating", "the", "similarity", "between", "datasets")
  val s2: Array[String] = Array("minhash", "is", "a", "probability", "data", "structure", "for",
    "estimating", "the", "similarity", "between", "documents")
  val s3: Array[String] = Array("cats", "are", "tall", "and", "have", "been",
    "known", "to", "sing", "quite", "loudly")


  test("testRandInt") {

  }

  test("testHashbands") {

  }

  test("testPermB") {

  }

  test("testSeed_$eq") {

  }

  test("testHashvalues_$eq") {

  }

  test("testHashbandsStr_$eq") {

  }

  test("testInitPermutations") {

  }

  test("testHashvalues") {

  }

  test("testPermB_$eq") {

  }

  test("Two same strings should get same hash value") {
    val testString = "This is a test string"
    val minhash = new Minhash().inithashvalues().initPermutations
    assert(minhash.hash(testString) == minhash.hash(testString))
  }

  test("Two difference strings should get very difference hash value - 1") {
    val testString1 = "This is a test string"
    val testString2 = "This is a test string."

    val minhash = new Minhash().inithashvalues().initPermutations
    assert(minhash.hash(testString1) != minhash.hash(testString2))
  }

  test("Two difference strings should get very difference hash value - 2") {
    val testString1 = "This is a test string"
    val testString2 = "This is - a test string"

    val minhash = new Minhash().inithashvalues().initPermutations
    assert(minhash.hash(testString1) != minhash.hash(testString2))
  }

  test("Two difference strings should get very difference hash value - 3") {
    val testString1 = "This is a test string"
    val testString2 = "This is ä test string"

    val minhash = new Minhash().inithashvalues().initPermutations
    assert(minhash.hash(testString1) != minhash.hash(testString2))
  }

  test("max hast") {
    val testString = ""
    val minhash = new Minhash().inithashvalues().initPermutations
    assert(minhash.hash(testString) == minhash.maxHash)
  }

  test("testInithashvalues") {

  }

  test("testNumPerm") {

  }

  test("testHashbands_$eq") {

  }

  test("testUpdate") {

  }

  test("testPermA") {

  }

  test("testSeed") {

  }

  test("testHashbandsStr") {

  }

  test("testJaccard") {

  }

  test("testPermA_$eq") {

  }

}
