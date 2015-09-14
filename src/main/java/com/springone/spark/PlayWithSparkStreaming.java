package com.springone.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springone.spark.utils.TwitterConnection;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

/**
 * Connection with the Twitter API and Spark streaming to retrieve stream of tweets.
 */
public class PlayWithSparkStreaming {
  final static Logger log = Logger.getLogger(PlayWithSparkStreaming.class);

  static JavaStreamingContext jssc;

  // WARNING: Change the path and keys
  // the path file where we will store the data
  private static String PATH = "/Users/ludwineprobst/DataSets/twitter/";

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

    String[] filters = {"spring", "springone", "java", "spark" , "#DC", "washington" , "#paris"};

    JavaDStream<String> tweets = TwitterUtils.createStream(jssc, TwitterConnection.getAuth(), filters)
                                             .map(tweetStatus -> mapper.writeValueAsString(tweetStatus));

    tweets.print(5);

    tweets.repartition(1).dstream().saveAsTextFiles(PATH, "stream");

    // Start the context
    jssc.start();
    jssc.awaitTermination();
  }

}
