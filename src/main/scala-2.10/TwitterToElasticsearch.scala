import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import twitter4j.GeoLocation
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkContext
import org.elasticsearch.spark._


object TwitterToElasticsearch {

  def parsingTweets(tweetId:Long, tweetDate:Date, tweetText:String,
                    tweetUser:Long, tweetUserName: String, tweetLocation: GeoLocation)
  : Map[String, Object] = {

    val outputFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS")
    val _timestamp = outputFormat.format(tweetDate)
    val tweetMap = Map(
      "id" -> tweetId.toString,
      "@timestamp" -> _timestamp.toString,
      "text" -> tweetText.toString,
      "user_id" -> tweetUser.toString,
      "user_name" -> tweetUserName,
      "location" -> Array(tweetLocation.getLatitude.toString,
        ',', tweetLocation.getLongitude.toString).mkString
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
    val SPARK_MASTER = properties.get("spark_master").asInstanceOf[String]
    val ES_HOST = properties.get("elasticsearch_host").asInstanceOf[String]
    val ES_PORT = properties.get("elasticsearch_port").asInstanceOf[String]

    val conf = new SparkConf()
      .setMaster(SPARK_MASTER)
      .setAppName("TwitterToES")
      .set("es.port", ES_PORT)
      .set("es.nodes",ES_HOST)
      .set("es.index.auto.create", "false")

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

    val filteredTweet = tweetStream.filter(
      tweet => tweet.getLang.equals("en")
      && tweet.isRetweet.equals(false)
      && tweet.getGeoLocation != null
    )

    val parsedTweet = filteredTweet.map(
      tweet => parsingTweets(tweet.getId, tweet.getCreatedAt,
                             tweet.getText, tweet.getUser.getId,
                             tweet.getUser.getName, tweet.getGeoLocation))


    parsedTweet.foreachRDD(rdd =>
      rdd.saveToEs("twitter/tweets", Map("es.mapping.id" -> "id")))

    ssc.start()
    ssc.awaitTermination()
  }
}
