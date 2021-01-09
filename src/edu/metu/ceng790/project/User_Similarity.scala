package edu.metu.ceng790.project

import org.apache.spark.rdd.RDD
import util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.recommendation.Rating
import java.util.NoSuchElementException

object User_Similarity {
	def userSimilarity(userVectorA: Map[String,Int], userVectorB: Map[String,Int]) : Double = {
			var AB : Double = 0 
			var A : Double = 0 
			var B : Double = 0 
					for (elem <- userVectorA) {
						breakable 
						{
						  val x = elem._2
							var y = 0
						try {
							  y = userVectorB(elem._1)
						} catch {
						case key : NoSuchElementException =>  break 
						}
							AB += x * y
							A += Math.sqrt(x)
						}
					}
	for (elem <- userVectorB) {
		B += Math.sqrt(elem._2)
	}
	val sqrtA = math.sqrt(A)
			val sqrtB = math.sqrt(B)

			if ( sqrtA == 0 || sqrtB == 0){
				return 0.0
			}      
	return AB / (sqrtA * sqrtB)
	}

	def knn(testUser : Map[String, Int], k:Int, usersVectors : RDD[(Int, Map[String, Int])] ) : ArrayBuffer[(Int, Double)] = {
			var userIdSimilarities = ArrayBuffer.empty[(Int, Double)]
					for (user <- usersVectors.collectAsMap()) {
						val sim = userSimilarity(testUser, user._2)
								userIdSimilarities += ((user._1, sim))
					}
			val similarUsers = userIdSimilarities.sortBy(x => x)(Ordering[Double].reverse.on(_._2)).take(k)

					return similarUsers
	}
		def prodcutRecommend(similarUsers:ArrayBuffer[(Int, Double)],
			productNames:Map[Int, String],
			goodRatings:Map[Int, Iterable[Rating]] ) : ArrayBuffer[(String, Double)] = {

					var productRatings = ArrayBuffer.empty[(String, Double)]
							for ( product <- productNames ) {
								var weightedRating = 0.0
										var totalSimilarity = 0.0
										for ( user <- similarUsers.toMap )  {
											var rating = goodRatings(user._1).filter(_.product == product._1)
													rating.foreach( rat => { weightedRating +=  user._2 * rat.rating; totalSimilarity += user._2 } )
										}
								//println(weightedRating, totalSimilarity)
								if(totalSimilarity != 0) {
									val possibleRating = weightedRating / totalSimilarity
											productRatings += ((product._2, possibleRating))
								}
							}
					val recommendations = productRatings.sortBy(x => x)(Ordering[Double].reverse.on(_._2)).take(10)
							return recommendations
	}

}