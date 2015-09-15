package com.springone.spark;


import com.springone.spark.utils.NGram;
import com.springone.spark.utils.TwitterConnection;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;

import java.util.List;

/**
 * Use the Kmeans method to define cluster per language.
 */
public class KmeanModel {

  final static Logger log = Logger.getLogger(KmeanModel.class);

  private static String pathToFile = "file:///Users/ludwineprobst/DataSets/twitter/*";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("K-means")
        .setMaster("local[*]"); // here local mode. And * means you will use as much as you have cores.

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(2));
    SQLContext sqlContext = new SQLContext(sc);

    // need to build the data :D :D
    DataFrame tweets = sqlContext.jsonFile(pathToFile);
    tweets.registerTempTable("tweets");

    DataFrame dataFrame = sqlContext.sql("SELECT lang, text FROM tweets WHERE lang in ('en', 'es', 'ja')");
    JavaPairRDD<String, String> couple = dataFrame.javaRDD().mapToPair(row -> new Tuple2(row.get(0).toString(), row.get(1).toString()));

    System.out.println("first element of the sql request : " + couple.first()._1() + " " + couple.first()._2());
    System.out.println("sql request count : " + couple.count());

    JavaRDD<String> texts = couple.map(e -> e._2());
    // remove some special caracters...url, # and @ mentions
    // http://stackoverflow.com/questions/161738/what-is-the-best-regular-expression-to-check-if-a-string-is-a-valid-url
    JavaRDD<String> points = texts
        .map(e -> e.toLowerCase())
        .map(e -> e.replaceAll("rt\\s+", ""))
        .map(e -> e.replaceAll(":", ""))
        .map(e -> e.replaceAll("!", ""))
        .map(e -> e.replaceAll(",", ""))
        .map(e -> e.replaceAll("\\s+#\\w+", ""))
        .map(e -> e.replaceAll("#\\w+", ""))
        .map(e -> e.replaceAll("(?:https?|http?)://[\\w/%.-]+", ""))
        .map(e -> e.replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", ""))
        .map(e -> e.replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", ""))
        .map(e -> e.replaceAll("(?:https?|http?)//[\\w/%.-]+", ""))
        .map(e -> e.replaceAll("\\s+@\\w+", ""))
        .map(e -> e.replaceAll("@\\w+", ""))
        .map(e -> e.replaceFirst("\\s+", ""))
        .filter(e -> e.length() > 80);

    System.out.println("Point first: " + points.first());
    System.out.println("Point take: " + points.take(10));
    System.out.println("count " + points.count());

    List<String> tests = points.take(100);

    // slip into 2-gram
    JavaRDD<Iterable<String>> lists = points.map(ele -> NGram.ngrams(2, ele));

    System.out.println("With ngram: " + lists.first());

    // https://en.wikipedia.org/wiki/Feature_hashing
    HashingTF hash = new HashingTF(1000);
    RDD<Vector> vectors = lists.map(line -> hash.transform(line)).rdd().cache();

    System.out.println("Vectors count: " + vectors.count());

    int clusterNumber = 3;
    int iter = 20;

    KMeansModel model = KMeans.train(vectors, clusterNumber, iter);

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    double wssse = model.computeCost(vectors);
    System.out.println("Within Set Sum of Squared Errors = " + wssse);

    for (String test: tests) {
      Iterable<String> ngram = NGram.ngrams(2, test);
      Vector v = hash.transform(ngram);
      int cluster = model.predict(v);
      System.out.println(test + " is in the cluster " + cluster);
    }

    JavaDStream<String> contents = TwitterUtils.createStream(jssc, TwitterConnection.getAuth())
                                               .map(tweet -> tweet.getText());


    /*sc.parallelize(Arrays.asList(model.clusterCenters())).saveAsObjectFile("/Users/ludwineprobst/springone/model");

    JavaRDD<Object> modelSaved = sc.objectFile("/Users/ludwineprobst/springone/model");
    KMeansModel reg = new KMeansModel(modelSaved.rdd().collect());

    JavaDStream<String> results = contents.filter(content -> reg.predict(hash.transform(NGram.ngrams(2, content))) == clusterNumber);

    results.print();*/
  }

}
