package utils

import org.scalatest.FunSuite

class TokenizerTest extends FunSuite {
  test("Normal string, words are already lowercase") {
    val text = "this is a test"
    assert(Tokenizer.tokenize(text).deep == Array("this", "is", "a", "test").deep)
  }

  test("Some words are uppercase") {
    val text = "This is A Test"
    assert(Tokenizer.tokenize(text).deep == Array("this", "is", "a", "test").deep)
  }

  test("Dots, commas, semicolon should be filter out") {
    val text = "This is a test, and more; test."
    assert(Tokenizer.tokenize(text).deep == Array("this", "is", "a", "test", "and", "more", "test").deep)
  }

  test("Slash should be filtered outa") {
    val text = "This is a test/ \\and more test"
    assert(Tokenizer.tokenize(text).deep == Array("this", "is", "a", "test", "and", "more", "test").deep)
  }

  test("Special character like @, % & should be filtered out") {
    val text = "This &is a test/ \\and@ €more %test"
    assert(Tokenizer.tokenize(text).deep == Array("this", "is", "a", "test", "and", "more", "test").deep)
  }
}
