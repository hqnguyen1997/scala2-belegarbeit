package search

object Tf {
  def tf(tokens: Array[(String)]): Map[String, Int] = {
    //es wird eine Map erstellt, Token -> Url, Vorkommenshäufigkeit
    tokens.map(token => (token, 1)).groupBy(x => x._1).map(x => (x._1, x._2.size))
  }

}
