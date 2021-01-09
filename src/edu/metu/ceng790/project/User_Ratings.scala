package edu.metu.ceng790.project

import java.io.FileWriter
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg.shuffle
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object User_Ratings {
	/* case class Rating(userID: String, productID: String,
		rating: Int)*/

	// Create Rating object from Row
	def parseRating(fields: Row): Rating = {
			Rating(fields(0).toString().hashCode(), fields(4).toString.hashCode(), fields(18).toString().toDouble)
	}

	// Read ratings from csv file and create RDD[Rating]
	def readRatings(location:String, spark: SparkSession): RDD[Rating] = {
			val ratings = spark.read.option("header", "true").csv(location).rdd.map(parseRating)
					return ratings
	}

	// Normalizing the  ratings by dividing user's rating to average of user's ratings
	def normalizingRatings(ratings : RDD[Rating]) : RDD[Rating] = {
			// Grouping according to user.
			val ratingsofUsers = ratings.groupBy(f => f.user).map( x => (x._1, x._2.map( r => r.rating).sum / x._2.size ) )

					// Collecting as Map
					val userMap = ratingsofUsers.collect().toMap

					// Normalizing the Ratings
					val normalizedRatings = ratings.map( f => Rating(f.user, f.product, f.rating / userMap(f.user) ) )

					return normalizedRatings
	}

	def main(args: Array[String]): Unit = {

			var spark: SparkSession = null
					var fw :FileWriter = null

					try {

						spark = SparkSession.builder.appName("Spark SQL").config("spark.master", "local[*]").getOrCreate()
								val sc = spark.sparkContext		
								val sqlContext = new org.apache.spark.sql.SQLContext(sc)
								Logger.getLogger("org").setLevel(Level.OFF)
								Logger.getLogger("akka").setLevel(Level.OFF)


					} catch {
					case e: Exception => throw e
					} finally {
						val end = System.nanoTime()
								spark.stop()
								fw.close()
					}
	println("done")
	}
}

/*
       Reference : https://spark.apache.org/docs/latest/ml-collaborative-filtering.html
 */