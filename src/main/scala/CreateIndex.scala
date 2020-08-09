import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import stemmers.{EnglishStemmer, GermanStemmer}
import utils.{LanguageDetector, Tokenizer}


object CreateIndex {
  def main(args: Array[String]): Unit = {

    val importFileName="smallTestSample.csv"
    val exportFileName="invertedIndex.bin"

    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val csvData = sc.textFile(importFileName)
    //Zeilen werden gezählt
    println("Documents Total:"+csvData.count())
    //Startzeit wird gemessen

    val t1 = System.nanoTime()
    val rows:RDD[(String,String)]=csvData.map(line => {
      //Zeilen lesen
      val splitted:Array[String]=line.split(";")
      //aus der Zeile Vorkommenshäufigkeit und Url entnehmen
      if(splitted.size==2)(splitted(0),splitted(1)) else ("","")
    })
    //url , content
    val tokenized = rows.map(rec => (rec._1, Tokenizer.tokenize(rec._2)))

    //url , tokens , Langcode
    val langaugesDetected = tokenized.map(rec=>(rec._1,rec._2,LanguageDetector.detect(rec._2)))

    val preferredLanguages = List(LanguageDetector.LANGCODE_GERMAN,LanguageDetector.LANGCODE_ENGLISH)
    //nur unsere bevorzugten sprachen werden herausgenommen

    val filteredLanguages=langaugesDetected.filter(preferredLanguages contains _._3)
    //Stopworte werden gefiltert
    val cleanData=filteredLanguages.map(rec=>(rec._1,StopwordFilter.filter(rec._2,rec._3),rec._3))
    //url, stemmedTokens
    val stemmedData=cleanData.map(rec=>(
      rec._1,
      if(rec._3==LanguageDetector.LANGCODE_GERMAN) rec._2.map(word=>GermanStemmer.stem(word))
      else rec._2.map(word=>EnglishStemmer.stem(word))
      ))

    //term frequency wird ermittelt
    val tf=stemmedData.map(rec=>(rec._1->Tf.tf(rec._2)))
    val documentsSize=tf.count.toInt
    //Invertierter Index wird erstellt
    val invert=tf .flatMap(tuple => tuple._2.map(x => (x._1, (tuple._1, x._2)))).mapValues(Map(_)).reduceByKey(_ ++ _)

    //Dauer für die Erstellung wird ermittelt
    println("Indexiert: " + (System.nanoTime() - t1)/1e+6/1000  + " Sek.")
    val t2= System.nanoTime()
    //Index wird gespeichert
    val invertedIndex=new InvertedIndex(invert.collect().toMap,documentsSize).saveToFile(exportFileName)
    println("Datei gespeichert:"+exportFileName +" "+ (System.nanoTime() - t2)/1e+6/1000  + " Sek.")
    println("Anzahl der Dokumente:"+documentsSize)




  }
}
