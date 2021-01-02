package edu.metu.ceng790.project

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level


/**
  * Created by Alper Emre HAS on 02/01/21.
  */
/*
 * DATASET COLUMNS
 * id,
 * dateAdded,
 * dateUpdated,
 * name,
 * asins,
 * brand,
 * categories,
 * primaryCategories,
 * imageURLs,
 * keys,
 * manufacturer,
 * manufacturerNumber,
 * reviews.date,
 * reviews.dateSeen,
 * reviews.didPurchase,
 * reviews.doRecommend,
 * reviews.id,
 * reviews.numHelpful,
 * reviews.rating,
 * reviews.sourceURLs,
 * reviews.text,
 * reviews.title,
 * reviews.username,
 * sourceURLs*/
object CollabarativeFiltering {
  case class Product(
      )
   def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("Spark SQL").config("spark.master", "local[*]").getOrCreate()
			val sc = spark.sparkContext
			val sqlContext = new org.apache.spark.sql.SQLContext(sc)
			Logger.getLogger("org").setLevel(Level.OFF)
			Logger.getLogger("akka").setLevel(Level.OFF)
			//Loading the Data
			val DATA : RDD[String] = spark.sparkContext.textFile("Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products_May19.csv")
			val first_header = DATA.first()
			val dataRDD = DATA.filter(row => row != first_header).map{ fields => fields.split(",")}
			// Showing the Ten Data
		
		dataRDD.collect().foreach(println)

			
		
	}
}
