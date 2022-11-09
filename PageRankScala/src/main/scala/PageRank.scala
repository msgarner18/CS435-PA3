import org.apache.spark.sql.SparkSession


object PageRank {

  def main(args: Array[String]): Unit = {

    // uncomment below line and change the placeholders accordingly
    val sc = SparkSession.builder().master("spark://salem:30120").getOrCreate().sparkContext

    // to run locally in IDE,
    // But comment out when creating the jar to run on cluster
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext

    // to run with yarn, but this will be quite slow, if you like try it too
    // when running on the cluster make sure to use "--master yarn" option
//    val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext

    val lines = sc.textFile(args(0))  
    val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))
    val numLinks = links.count()
    var ranks = links.mapValues(v => 1.0 / numLinks) 
    ranks.saveAsTextFile(args(2)+ "ranks")
    links.collect().foreach(println)
    ranks.collect().foreach(println)
    for (i <-1 to 25){
        val tempRank = links.join(ranks).values.flatMap {  
            case (urls, rank)=>
            val outgoingLinks = urls.split(" ")
            outgoingLinks.map(url => (url, rank /outgoingLinks.length))
        }
        // Updated ranks: { (B, _), (C, _), (D, _), (A, _), ... } 
        ranks = tempRank.reduceByKey(_+_)
    }
    ranks.saveAsTextFile(args(2) + "updatedRanks")

  }
 
}