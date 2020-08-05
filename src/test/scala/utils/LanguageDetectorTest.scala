package utils

import org.scalatest.FunSuite

class LanguageDetectorTest extends FunSuite {

  test("Detect english") {
    assert(LanguageDetector.detect(Array("This", "is", "english", "language")) == "en")
  }

  test("Detect german") {
    assert(LanguageDetector.detect(Array("Das", "ist", "deutsche", "Sprache")) == "de")
  }

  test("Detect spanish") {
    assert(LanguageDetector.detect(Array("Hoy", "es", "un", "buen", "día", "me", "gustaría", "una", "cerveza")) == "es")
  }
}
