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
		productCat: Array[String])

def parseProduct(fields: Row): Product = {		
		Product(fields(4).toString().hashCode(),
				fields(3).toString(),
				fields(7).toString().split("\\,"))

}
def readProduct(location:String, spark: SparkSession): RDD[Product] = {
		var input_product = spark.read.option("header", "true").csv(location).rdd.map(parseProduct).distinct().cache()
				val product = input_product.distinct()
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
						val topRatedProduct = topRatedProducts(products, ratings, 5)
						topRatedProduct.take(5).foreach(println)

						// Ask user to rate 10 top rated product
						val ourRatings = getRatings(topRatedProduct, spark)

						// Add User Ratings
						val editedRatings = ratings.union(ourRatings)

						//Normalizing the Ratings
						val normalizedRatings = User_Ratings.normalizingRatings(editedRatings)

						//Splitting The Data into Training and Test Data
						val Array(training_data, test_data) = normalizedRatings.randomSplit(Array(0.8, 0.2))
						training_data.distinct().cache()
						test_data.distinct().cache()

						// Training the model
						val model = ALS.train(training_data, 4, 10 , 0.01)

						// Clearing the actual ratings of user's to make our predictions
						val bareProduct = test_data.map(f => (f.user, f.product))

						// Predicting the user's rating with our trained 
						val modelPredictions = model.predict(bareProduct).map(p => ((p.user, p.product), p.rating))

						// The Product (user, product) is a key and (actual rating, predicted rating) is pair 
						val combinedPredictions = test_data.map(f => ((f.user, f.product),f.rating)).join(modelPredictions)
						println("UserID---ProductID---Rating---Predicted_Rating ")
						combinedPredictions.collect().take(10).foreach(println)

						// Recommend 10 product
						val recommendations = model.recommendProducts(0, 10)

						/*Print recommended product
						var t = recommendations.map(f => f.product == products.filter(p => p.prooductID == f.product).id)*/

						//recommendations.foreach(println)

						var products_df = spark.createDataFrame(products.distinct()).toDF("prooductID", "prodcutName", "productCat")
						print(products_df)
						products_df.show()

						var rdd = spark.sparkContext.parallelize(recommendations)

						var recommendations_df = spark.createDataFrame(rdd).toDF("user", "product", "rating")
						print(recommendations_df.schema)
						recommendations_df.show()


						val joined_df = recommendations_df.join(products_df, col("product") === col("prooductID"), "inner")
						joined_df.show()

						/*for( a <- recommendations ){
						var t = products.filter(f => f.prooductID == a.product).distinct()
						}
						var t = recommendations.map(f => products.filter(p => p.prooductID == f.product))
						.take(1)(0)
						.foreach(y => y.collect().foreach(println))*/

						//Product category with productID
						val productCat = products.map(p => (p.prooductID, p.productCat))
						println("\nCategories of 10 Product:")					
            productCat.map { case (a , arr) => (a, arr.toList) }.take(10).foreach(println)

						//User Average Ratings
						val avgRatings = ratings.groupBy(r => r.user).map(x => (x._1, x._2.map(c => c.rating/x._2.size).reduce((a,b) => (a+b)), x._2))
						println("\nAverageRating of 10 user:")					
            avgRatings.map { case (a ,b, arr) => (a, b, arr.toList) }.take(10).foreach(println)
						
						
						//Flattened AverageRating
						val avgRatingsFlatened = avgRatings.flatMap(x=> (x._3.filter(y => y.rating >= x._2)))
						println("\nFlattened Avg Ratings of 10 :")	
						avgRatingsFlatened.take(10).foreach(println)

				
						// productId and it's rating acording to categorty
						val ratedProductCat = avgRatingsFlatened.map(rat => (rat.product, rat))
						println("\n Product Category Rating :")	
						ratedProductCat.take(10).foreach(println)

						// Flatten the ratings for each user with category					
						val userInterest = ratedProductCat.join(productCat).map(x => (x._2._1.user, x._2._2)).groupByKey()
						println("\n User Interest Combined and Flatened:")			
						//***************Aşağıdaki ile bastırdım ama istediğim şekilde bastıramadım ve çok uzun sürdü.
            //userInterest.map { case (a , arr) => (a, arr.toList) }.take(10).foreach(println)

						// Calculating the UserInterest over the data
						val userInterestCount = userInterest.map(u => (u._1, u._2.flatMap(interest => interest.map(cat => (cat,1) )  ) ))
/*
 * HATAYI AŞAĞIDAKİ KOD İLE ALIYORUM BURADA collectAsMap İLE BELLEĞE ALDIĞI VERİ BOYUTU AŞIYOR SANIRIM.
						// UserInterest Vector
						val usersVector = userInterestCount.map(u => (u._1, u._2.groupBy(_._1).map(x=> (x._1, x._2.size) )))
						val user = usersVector.collectAsMap()(0)
						println("User's Interest vector")
						println(user)
						val similarUsers = User_Similarity.knn(user, 10, usersVector)
*/
			}

catch{
case e : Exception => throw e
}finally {
	spark.stop()
}
println("done")
}

}
