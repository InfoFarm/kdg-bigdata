import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ben on 24/06/15.
  */
object Recommender extends App {

  val sparkConf = new SparkConf().setMaster("local").setAppName("Napstify")

  val sparkContext = new SparkContext(sparkConf)

   val lines: RDD[String] = sparkContext.textFile("..//KdG-BigData/src/main/resources/distinctTweetsArtist.txt")
  val tweets: RDD[(String, String)] = lines.map(line => {
    val splits = line.split("\t", 2)
      (splits(0), splits(1))
  })

  //make artist pairs per user
   val joinedTweets = tweets.join(tweets)
  //filter duplicate pairs (artist A - artist B is the same as  artist B - artist A)
  val artistPairsWithUser = joinedTweets.filter{case (user, (artist1, artist2)) => artist1 < artist2}
  val artistPairCounts: RDD[((String, String), Int)] =
    artistPairsWithUser.map{case (user, (artist1, artist2)) =>
      ((artist1, artist2), 1)}
      .reduceByKey((count, totalCount) => totalCount + count)

  private val orderedPairs: RDD[((String, String), Int)] = artistPairCounts.sortBy{pairCount => -pairCount._2}

  private val recommendations: Array[((String, String), Int)] = orderedPairs.take(50)
  for(recommendation <- recommendations) {
    println(recommendation)
  }


}
