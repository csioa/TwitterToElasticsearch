import java.io.FileInputStream
import java.util.{Date, Properties}

import twitter4j.{GeoLocation, StatusJSONImpl, User}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkContext
import org.elasticsearch.spark._

import scala.collection.immutable.HashMap


object TwitterToElasticsearch {

  def parsingTweets(tweetId:Long, tweetDate:Date, tweetText:String, tweetUser:Long, tweetUserName: String, tweetLocation: GeoLocation) : Map[String, String] = {
    val tweetMap = Map(
      "id" -> tweetId.toString,
      "date" -> tweetDate.toString,
      "text" -> tweetText.toString,
      "user_id" -> tweetUser.toString,
      "user_name" -> tweetUserName,
      "location" -> tweetLocation.toString
    )
    tweetMap
  }

  // Reading the Twitter API credentials from a properties file
  val properties = new Properties()
  try{
    properties.load(new FileInputStream("credentials.properties"))
  }
  catch {
    case e: java.io.FileNotFoundException =>
      println("The properties file specified, was not found!")
      System.exit(1)
  }

  def main(args: Array[String]) {

    // Loading the properties from the file
    val consumer = properties.get("consumer").asInstanceOf[String]
    val consumer_secret = properties.get("consumer_secret").asInstanceOf[String]
    val access_token = properties.get("access_token").asInstanceOf[String]
    val access_token_secret = properties.get("access_token_secret").asInstanceOf[String]

    val conf = new SparkConf().setMaster("local[4]")
      .setAppName("TwitterToES")
      .set("es.port", "9200")
      .set("es.nodes","localhost")
      .set("es.index.auto.create", "true")

//    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(5))
//
    // Building the configuration for the Authenticator
    val config = new ConfigurationBuilder()
    config.setOAuthConsumerKey(consumer)
    config.setOAuthConsumerSecret(consumer_secret)
    config.setOAuthAccessToken(access_token)
    config.setOAuthAccessTokenSecret(access_token_secret)

    val auth = Some(new OAuthAuthorization(config.build()))

    // Instantiating the Twitter stream
    val tweetStream = TwitterUtils.createStream(ssc, auth)

    val englishTweet = tweetStream.filter(tweet => tweet.getLang.equals("en"))
    val noRetweet = englishTweet.filter(tweet => tweet.isRetweet.equals(false))
    val withLocation = noRetweet.filter(tweet => tweet.getGeoLocation != null)

    val parsedTweet = withLocation.map(tweet => parsingTweets(tweet.getId,
      tweet.getCreatedAt, tweet.getText, tweet.getUser.getId, tweet.getUser.getName, tweet.getGeoLocation))

//    parsedTweet.print(10)

    parsedTweet.foreachRDD(rdd => rdd.saveToEs("twitter/tweets", Map("es.mapping.id" -> "id")))

    ssc.start()
    ssc.awaitTermination()
  }
}
