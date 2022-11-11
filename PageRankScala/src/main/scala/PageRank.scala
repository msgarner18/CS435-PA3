// package main

import org.apache.spark.sql.SparkSession
// import main.Port

object PageRank {
  // private val 

  def main(args: Array[String]) = {

    // uncomment below line and change the placeholders accordingly
    val sc = SparkSession.builder().master("spark://salem:30361").getOrCreate().sparkContext;
    // to run locally in IDE,
    // But comment out when creating the jar to run on cluster
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext

    // to run with yarn, but this will be quite slow, if you like try it too
    // when running on the cluster make sure to use "--master yarn" option
//    val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext

    val titles = sc.textFile(args(1)).zipWithIndex().mapValues(x=>x+1).map(_.swap)
    for (j <- 1 to 2) {
      //-------WikiBomb-------
      val origLinks = sc.textFile(args(0)).map(s=>(s.split(": ")(0), s.split(": ")(1)))
      val links = if(j==1) origLinks else composeWikiBomb(args, titles, origLinks)
      val wikiString = if(j == 1) "" else "WikiBomb"
      if(j == 2) {
        links.coalesce(1).saveAsTextFile(args(2) + wikiString)
      }
      
      //-----Page Rank With and Without Taxation------
      for (i <-1 to 2){
        val ranks = if(i == 1) pageRankWithoutTaxation(links) else pageRankWithTaxation(links);

        //-----Output--------
        val pageRank = createPageRank(titles, ranks)
        
        val sortedPageRankArray = pageRank.sortBy(x => x._2, false).take(10)
        val sortedPageRank = sc.parallelize(sortedPageRankArray).coalesce(1)

        val outputFileName = if(i == 1) "pageRankWithoutTaxation" else "pageRankWithTaxation"
        sortedPageRank.saveAsTextFile(args(2) + outputFileName+wikiString)
      }
    }
    

    
  }

  def composeWikiBomb(args: Array[String], titles: org.apache.spark.rdd.RDD[(Long, String)], origLinks: org.apache.spark.rdd.RDD[(String, String)]) : org.apache.spark.rdd.RDD[(String, String)] = {
    // (id, title)
    val surfTitles = titles.filter(t=> t._2.toLowerCase.contains("surfing"))
    // surfTitles.saveAsTextFile(args(2) + "surfTitles")

    // (id, links)
    val numberedLinks = origLinks.zipWithIndex().mapValues(x=>x+1).map(_.swap)
    // numberedLinks.saveAsTextFile(args(2) + "numberedLinks")

    // (linkStartNode, LinkEndNodes)
    val surfLinks = surfTitles.join(numberedLinks)
    .values.map {  
      case (id, links)=>
        links
    }
    // surfLinks.saveAsTextFile(args(2) + "surfLinks")
    
    // (id, rocky mountain national park)
    val rockyTitle = titles.filter(t=> t._2.toLowerCase.contains("rocky mountain national park"))
    // rockyTitle.saveAsTextFile(args(2) + "rockyTitle")

    // string
    val rockyLinkKey = rockyTitle.join(numberedLinks).values.map {  
      case (title, link)=>
        link._1
    }.first()

    // alterSurfLinks such that each surf link includes rockyLinkKey as one of its LinkEndNodes
    val alteredSurfLinks = surfLinks.map {
      case (key, links)=>
        val linkArray = links.split(" ")
        if(linkArray.contains(rockyLinkKey)) {
          (key, linkArray.mkString(" "))
        } else {
          val alteredLinks = links + " " + rockyLinkKey
          val alteredLinksArray = alteredLinks.split(" ")
          val sortedAlteredLinksArray  = alteredLinksArray.sortBy(x => x)
          (key, sortedAlteredLinksArray.mkString(" "))
        }
    }
    // alteredSurfLinks.saveAsTextFile(args(2) + "alteredSurfLinks")

    // create links rdd with altered links (linkStartNode, LinkEndNodes)
    val surfLinksArray = surfLinks.collect()
    val alteredSurfLinksArray = alteredSurfLinks.collect()
    val links = numberedLinks.map {
      case (key, link) => {
        val index = surfLinksArray.indexOf(link)
        if(index == -1) {
          link
        } else {
          alteredSurfLinksArray.apply(index)
        }
      }
    }
    // links.saveAsTextFile(args(2) + "links")

    // return
    links
  }

  def pageRankWithTaxation(links: org.apache.spark.rdd.RDD[(String, String)]) : org.apache.spark.rdd.RDD[(String, Double)] = {
    val numLinks = links.count()
    var ranks = links.mapValues(v => 1.0 / numLinks)

    val b = 0.8
    for (i <-1 to 25){
        val tempRank = links.join(ranks).values.flatMap {  
          case (urls, rank)=>
            val outgoingLinks = urls.split(" ")
            outgoingLinks.map(url => (url, ((b * rank /outgoingLinks.length) + (1-b)*1/numLinks)))
        }
        ranks = tempRank.reduceByKey(_+_)
    }

    ranks.sortByKey()

    // return
    ranks
  }

  def pageRankWithoutTaxation(links: org.apache.spark.rdd.RDD[(String, String)]) : org.apache.spark.rdd.RDD[(String, Double)] = {
    val numLinks = links.count()
    var ranks = links.mapValues(v => 1.0 / numLinks)

    for (i <-1 to 25){
        val tempRank = links.join(ranks).values.flatMap {  
            case (urls, rank)=>
            val outgoingLinks = urls.split(" ")
            outgoingLinks.map(url => (url, rank /outgoingLinks.length))
        }
        // Updated ranks: { (B, _), (C, _), (D, _), (A, _), ... } 
        ranks = tempRank.reduceByKey(_+_)
    }

    ranks.sortByKey()

    ranks
  }

  def createPageRank(titles: org.apache.spark.rdd.RDD[(Long, String)], ranks: org.apache.spark.rdd.RDD[(String, Double)]) : org.apache.spark.rdd.RDD[(String, Double)] = {
    val updatedRanks = ranks.zipWithIndex().mapValues(x=>x+1).map(_.swap)
    //  updatedRanks.saveAsTextFile(args(2) + "updatedRanks")
    
    val pageRank = titles.join(updatedRanks).values.map {  
      case (title, rank)=>
        (title, rank._2)
    }

    pageRank
  }
}
  