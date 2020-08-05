import scala.collection.mutable.Map
class Minhash {
  // prime is the smallest prime larger than the largest
  // possible hash value (max hash = 32 bit int)
    private val prime = 4294967311l
  //Math.pow(2, 32) - 1
    private val maxHash =4294967295l
    var hashbands=scala.collection.mutable.ArrayBuffer.empty[Long]
    var hashbandsStr=scala.collection.mutable.ArrayBuffer.empty[String]
    private var numPerm=128
    private var seed = 1
     var hashvalues= scala.collection.mutable.ArrayBuffer.empty[Long]
    private var permA =  scala.collection.mutable.ArrayBuffer.empty[Int]
    private var permB =  scala.collection.mutable.ArrayBuffer.empty[Int]

    def inithashvalues:Unit={

      for( a <- 0 to this.numPerm){
        this.hashvalues+=this.maxHash
      }
    }

  // initialize the permutation functions for a & b
  // don't reuse any integers when making the functions
 def initPermutations :Unit= {

    var used:Map[Int,Boolean] = Map.empty
    for (i <- 0 to 1) {
      {

        var perms = scala.collection.mutable.ArrayBuffer.empty[Int]
        for (j <- 0 to this.numPerm) {
          var int:Int = this.randInt()
          while (used.exists(_ == int)) {
            int= this.randInt()
          }
          perms+=int

          used=used++ Map(int->true)
        }

        if (i == 0)
          this.permA = perms
        else
          this.permB = perms
      }
    }
  }
  def hash(str:String) :Long= {
    var hash = 0;
    if (str.length == 0) {
       hash + this.maxHash
    }
    for (i <- 0 to str.length-1) {
      var char = str.charAt(i)
      hash = ((hash<<5)-hash)+char
      hash = hash & hash // convert to a 32bit integer
    }
     hash + this.maxHash
  }

  def update(str:String) :Unit= {

    for (i <- 0 until this.hashvalues.length ) {

      var a:Long = this.permA(i)
      var b:Long = this.permB(i)

      var hash = (a * this.hash(str) + b) % this.prime
      if (hash < this.hashvalues(i)) {
        this.hashvalues(i) = hash
      }
    }
  }
  // estimate the jaccard similarity to another minhash

    def jaccard(other:Minhash) :Double= {
    if (this.hashvalues.length != other.hashvalues.length) {
        println("ashvalue counts differ")
    } else if (this.seed != other.seed) {
      println("seed values differ")
    }
    var shared = 0

      for (i <- 0 until this.hashvalues.length) {

   if( this.hashvalues(i) == other.hashvalues(i))
     shared=shared+1
    }
    return shared.toDouble / this.hashvalues.length.toDouble
  }


  def randInt() :Int= {
    this.seed=this.seed+1

    var x = Math.sin(  this.seed) * this.maxHash;
     Math.floor((x - Math.floor(x)) * this.maxHash).toInt
  }
  this.inithashvalues
  this.initPermutations

}
object Minhash{
  def main(args: Array[String]) {

    var s1:Array[String] = Array("minhash", "is", "a", "probabilistic", "data", "structure", "for",
      "estimating", "the", "similarity", "between", "datasets")
    var s2:Array[String] = Array("minhash", "is", "a", "probability", "data", "structure", "for",
      "estimating", "the", "similarity", "between", "documents")

    // create a hash for each set of words to compare
    var m1 = new Minhash
    var m2 = new Minhash

    // update each hash
    s1.map(w=> m1.update(w) )
    s2.map(w=> m2.update(w) )


    // estimate the jaccard similarity between two minhashes
    println( m1.jaccard(m2))

  }

}
