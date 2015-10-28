import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import twitter4j.Status

/**
 * Created by Ben on 17/10/15.
 */
object TwitterStreamingApp extends App {

  // Set the system properties so that Twitter4j library used by twitter stream
  // can use them to generate OAuth credentials
  setTwitterAuthProperties

  val sparkConf = new SparkConf().setAppName("TwitterStreaming")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  val filters = Array("#NowPlaying")
  val stream = TwitterUtils.createStream(ssc, None, filters)

  val regex = "^#NowPlaying .* (de|by|van|di) .* (de|from|van|da)?".r

  private val spotifyTweets: DStream[Status] = stream.filter(status => regex.findFirstIn(status.getText).isDefined)

  spotifyTweets.foreachRDD(rdd => rdd.foreach(status => println(status.getText)))

  ssc.start()
  ssc.awaitTermination()

  def setTwitterAuthProperties: String = {
    System.setProperty("twitter4j.oauth.consumerKey", "consumerKeyGoesHere")
    System.setProperty("twitter4j.oauth.consumerSecret", "consumerSecretGoesHere")
    System.setProperty("twitter4j.oauth.accessToken", "AccessTokenGoesHere")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "AccessTokenSecretGoesHere")
  }
}