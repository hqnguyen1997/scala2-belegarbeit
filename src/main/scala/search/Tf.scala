package search

object Tf {
  /**
   * es wird eine Map erstellt, Token -> Url, Vorkommenshäufigkeit
   * @param tokens
   * @return
   */
  def tf(tokens: Array[(String)]): Map[String, Int] = {
    tokens.map(token => (token, 1)).groupBy(x => x._1).map(x => (x._1, x._2.size))
  }

}
