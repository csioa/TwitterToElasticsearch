import java.io.FileInputStream
import java.util.Properties
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._


object TwitterToElasticsearch {

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

    val consumer = properties.get("consumer").asInstanceOf[String]
    val consumer_secret = properties.get("consumer_secret").asInstanceOf[String]
    val access_token = properties.get("access_token").asInstanceOf[String]
    val access_token_secret = properties.get("access_token_secret").asInstanceOf[String]

    val conf = new SparkConf().setMaster("local[4]")
      .setAppName("TwitterToES")
    val ssc = new StreamingContext(conf, Seconds(5))

    val config = new ConfigurationBuilder()
    config.setOAuthConsumerKey(consumer)
    config.setOAuthConsumerSecret(consumer_secret)
    config.setOAuthAccessToken(access_token)
    config.setOAuthAccessTokenSecret(access_token_secret)

    val auth = Some(new OAuthAuthorization(config.build()))

    val tweetStream = TwitterUtils.createStream(ssc, auth)

    tweetStream.print(10)

    ssc.start()
    ssc.awaitTermination()
  }

}
