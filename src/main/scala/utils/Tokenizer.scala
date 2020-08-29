package utils

object Tokenizer {
  /**
   * Tokenize string in array of word
   * all words should be lower case
   *
   * @param text
   * @return
   */
  def tokenize(text: String): Array[String] = {
    text.split("[\\p{Punct}\\p{Blank}\\s]{1,}").map(upperToken => upperToken.toLowerCase)
  }
}
