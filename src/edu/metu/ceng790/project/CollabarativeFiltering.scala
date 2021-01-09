package edu.metu.ceng790.project

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.util.control.Exception.Catch
import breeze.linalg.shuffle
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import com.github.fommil.netlib._


/**
 * Created by Alper Emre HAS on 02/01/21.
 */
/*
 * ----DATASET COLUMNS----
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
case class Product(prooductID: Int, prodcutName: String,
		productCat: String)

def parseProduct(fields: Row): Product = {
		//4,3,7,6
		Product(fields(4).toString().hashCode(),
				fields(3).toString(),
				fields(5).toString())

}
def readProduct(location:String, spark: SparkSession): RDD[Product] = {
		val product = spark.read.option("header", "true").csv(location).rdd.map(parseProduct)
				return product
}

def topRatedProducts(products : RDD[Product], ratings : RDD[Rating],  i: Int): Map[ Int, String] = {
		// Create mostRatedProducts(productID, Number_of_Product)
		val mostRatedProducts = ratings.groupBy(_.product).map(f=> (f._1, f._2.size)).takeOrdered(100)(Ordering[Int].reverse.on(_._2))

				// Select 100 of the top rated Products
				val selectedProdcut = shuffle(mostRatedProducts).map(f => (f._2, products.filter(_.prooductID == f._1)
						.map(p => p.prodcutName )
						.take(1)(0) ) ).take(i).toMap
				return selectedProdcut
}

def getRatings(topRatedProduct: Map[Int, String], spark: SparkSession): RDD[Rating] = {
		var ourId = 0
				var ourRatings  = ArrayBuffer.empty[Rating]
						var i = 1
						for(product <- topRatedProduct) {
							breakable {
								while(true) {
									try {
										println(i.toString + ") Your rating for: " + product._2 + "," )
										val rating = scala.io.StdIn.readInt()
										if(rating < 5.1 && rating > 0) {
											ourRatings += Rating(0, product._1, rating)
													i += 1
													break
										}
									} catch {
									case e: Exception => println("Invalid Rating");
									}
								}
							}
						}
		return spark.sparkContext.parallelize(ourRatings)
}
def main(args: Array[String]) {
	var spark : SparkSession = null
			try{

				spark = SparkSession.builder.appName("Spark SQL").config("spark.master", "local[*]").getOrCreate()
						val sc = spark.sparkContext
						var csv_file = "Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products_May19.csv"
						val sqlContext = new org.apache.spark.sql.SQLContext(sc)
						Logger.getLogger("org").setLevel(Level.OFF)
						Logger.getLogger("akka").setLevel(Level.OFF)

						//Loading Products
						val products = CollabarativeFiltering.readProduct(csv_file, spark)
						products.cache()

						products.take(10).foreach(println)

						//Loading Ratings
						val ratings = User_Ratings.readRatings(csv_file, spark)
						ratings.take(10).foreach(println)

						//Checking  Top Rated Products

						val topRatedProduct = topRatedProducts(products, ratings, 10)
						topRatedProduct.take(10).foreach(println)

						// Ask user to rate 10 top rated product
						val ourRatings = getRatings(topRatedProduct, spark)

						// Add User Ratings
						val editedRatings = ratings.union(ourRatings)

						//Normalizing the Ratings
						val normalizedRatings = User_Ratings.normalizingRatings(editedRatings)

						// Training the model
						val model = ALS.train(normalizedRatings, 4, 10 , 0.01)

						// Recommend 10 product
						val recommendations = model.recommendProducts(0, 10)

						// Print recommended product
						recommendations.map(f => products.filter(p => p.prooductID == f.product)
						.take(1)(0).prodcutName)
						.foreach(println)



			}

catch{
case e : Exception => throw e
}finally {
	spark.stop()
}
println("done")
}
}
