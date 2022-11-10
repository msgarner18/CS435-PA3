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
    //ranks.saveAsTextFile(args(2)+ "ranks")
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
    
    val titles = sc.textFile(args(1)).zipWithIndex().mapValues(x=>x+1).map(_.swap)
    // titles.saveAsTextFile(args(2) + "titles")

    ranks.sortByKey()
    val updatedRanks = ranks.zipWithIndex().mapValues(x=>x+1).map(_.swap)
    // updatedRanks.saveAsTextFile(args(2) + "updatedRanks")
    
    val pageRank = titles.join(updatedRanks).values.flatMap {  
      case (title, rank)=>
      val outgoing = title.split(" ")
      outgoing.map(t => (t, rank._2))
    }
    // pageRank.saveAsTextFile(args(2) + "pageRank")

    val sortedPageRankArray = pageRank.sortBy(x => x._2, false).take(10)
    val sortedPageRank = sc.parallelize(sortedPageRankArray).coalesce(1)
    sortedPageRank.saveAsTextFile(args(2) + "sortedPageRank")

    //-----START OF TAXATION ------
    val lines2 = sc.textFile(args(0))  
    val links2 = lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))
    //links.saveAsTextFile(args(2) + "links")
    val numLinks2 = links.count()
    var ranks2 = links.mapValues(v => 1.0 / numLinks) 
    val b = 0.8
    for (i <-1 to 25){
        val tempRank = links.join(ranks).values.flatMap {  
            case (urls, rank)=>
            val outgoingLinks = urls.split(" ")
              //outgoingLinks.map(url => (url, rank /outgoingLinks.length))
            outgoingLinks.map(url => (url, ((b * rank /outgoingLinks.length) + (1-b)*1/numLinks)))
        }
        // Updated ranks: { (B, _), (C, _), (D, _), (A, _), ... } 
        ranks2 = tempRank.reduceByKey(_+_)
        //ranks.saveAsTextFile(args(2) + "taxatedRates-" + i)
    }
    //ranks2.saveAsTextFile(args(2) + "rankTaxation")
    val titles2 = sc.textFile(args(1)).zipWithIndex().mapValues(x=>x+1).map(_.swap)
    //titles2.saveAsTextFile(args(2) + "titles")

    ranks2.sortByKey()
    val updatedRanks2 = ranks2.zipWithIndex().mapValues(x=>x+1).map(_.swap)
    //updatedRanks2.saveAsTextFile(args(2) + "updatedRanks")

    val pageRank2 = titles2.join(updatedRanks2).values.flatMap {  
      case (title, rank)=>
      val outgoing = title.split(" ")
      outgoing.map(t => (t, rank._2))
    }
    //pageRank2.saveAsTextFile(args(2) + "pageRank")

    val sortedPageRankArray2 = pageRank2.sortBy(x => x._2, false).take(10)
    val sortedPageRank2 = sc.parallelize(sortedPageRankArray2).coalesce(1)
    sortedPageRank2.saveAsTextFile(args(2) + "sortedPageRankWithTaxation")
  }
}
