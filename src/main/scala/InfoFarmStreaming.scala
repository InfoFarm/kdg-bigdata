import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

/**
 * Created by Ben on 17/10/15.
 */
object InfoFarmStreaming extends App {

  // Set the system properties so that Twitter4j library used by twitter stream
  // can use them to generate OAuth credentials
  setTwitterAuthProperties

  val sparkConf = new SparkConf().setAppName("InfoFarmStreaming")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  val filters = Array("InfoFarm", "InfoFarm_be")
  val stream = TwitterUtils.createStream(ssc, None, filters)

  stream.foreachRDD(rdd => rdd.foreach(status => println(status.getUser.getName + ": " + status.getText)))

  ssc.start()
  ssc.awaitTermination()

  def setTwitterAuthProperties: String = {
    System.setProperty("twitter4j.oauth.consumerKey", "consumerKeyGoesHere")
    System.setProperty("twitter4j.oauth.consumerSecret", "consumerSecretGoesHere")
    System.setProperty("twitter4j.oauth.accessToken", "AccessTokenGoesHere")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "AccessTokenSecretGoesHere")
  }
}