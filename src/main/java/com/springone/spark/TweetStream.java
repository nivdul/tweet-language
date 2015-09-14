package com.springone.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Connection with the Twitter API and Spark streaming to retrieve stream of tweets.
 */
public class TweetStream {
  final static Logger log = Logger.getLogger(TweetStream.class);

  static JavaStreamingContext jssc;

  // WARNING: Change the path and keys
  // the path file where we will store the data
  private static String PATH = "/Users/ludwineprobst/DataSets/twitter/";
  //
  // Class to authenticate with the Twitter streaming API.
  //
  // Go to https://apps.twitter.com/
  // Create your application and then get your own credentials (keys and access tokens tab)
  //
  // See https://databricks-training.s3.amazonaws.com/realtime-processing-with-spark-streaming.html
  // for help.
  //
  // If you have the following error "error 401 Unauthorized":
  // - it might be because of wrong credentials
  // OR
  //  - a time zone issue (so be certain that the time zone on your computer is the good one)
  private static String CONSUMER_KEY = "AFiNChb80vxYZfhPls2DXyDpF";
  private static String CONSUMER_SECRET = "JRg7SWyVFkXEESWbEzFzC1xaIGRC3xNdTvrekMvMFk6tjKooOR";
  private static String ACCESS_TOKEN = "493498548-HCCt6LCposCb3Ij7Ygt7ssTxTBPwGoPrnkkDQoaN";
  private static String ACCESS_TOKEN_SECRET = "3pxA3rnBzWa9bmOmOQPWNMpYc4qdOrOdxGFgp6XiCkEKH";

  public static OAuthAuthorization getAuth() {

    return new OAuthAuthorization(
        new ConfigurationBuilder().setOAuthConsumerKey(CONSUMER_KEY)
            .setOAuthConsumerSecret(CONSUMER_SECRET)
            .setOAuthAccessToken(ACCESS_TOKEN)
            .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
            .build());
  }

  public static void main(String[] args) {
    // create spark configuration and spark context: the Spark context is the entry point in Spark.
    // It represents the connexion to Spark and it is the place where you can configure the common properties
    // like the app name, the master url, memories allocation...
    SparkConf conf = new SparkConf()
        .setAppName("Playing with Spark Streaming")
        .setMaster("local[*]");  // here local mode. And * means you will use as much as you have cores.

    // create a java streaming context and define the window (2 seconds batch)
    jssc = new JavaStreamingContext(conf, Durations.seconds(2));

    System.out.println("Initializing Twitter stream...");

    ObjectMapper mapper = new ObjectMapper();

    /*JavaDStream<String> tweets = TwitterUtils.createStream(jssc, getAuth())
        .map(tweetStatus ->
            new Tweet(tweetStatus.getLang(),
                tweetStatus.getUser().getId(),
                tweetStatus.getUser().getName(),
                tweetStatus.getText()))
        .map(tweet -> mapper.writeValueAsString(tweet));*/

    String[] filters = {"spring", "springone", "java", "spark" , "#DC", "washington" , "#paris", "donald trump"};

    JavaDStream<String> tweets = TwitterUtils.createStream(jssc, getAuth(), filters)
                                             .map(tweetStatus -> mapper.writeValueAsString(tweetStatus));

    tweets.print();

    tweets.repartition(1).dstream().saveAsTextFiles(PATH, "stream");

    // Start the context
    jssc.start();
    jssc.awaitTermination();
  }

}
