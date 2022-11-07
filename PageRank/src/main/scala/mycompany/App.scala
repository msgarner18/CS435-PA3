package com.mycompany
// import org.apache.spark
import org.apache.spark.sql.SparkSession

object App {
  
  def main(args : Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    println("Hello World!")
    // val sc = SparkSession.builder().master("spark://pierre:30361").getOrCreate().sparkContext
    spark.stop()
  }

}
