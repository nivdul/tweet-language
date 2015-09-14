package com.springone.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 *  The Spark SQL and DataFrame documentation is available on:
 *  https://spark.apache.org/docs/1.4.0/sql-programming-guide.html
 *
 *  A DataFrame is a distributed collection of data organized into named columns.
 *  The entry point before to use the DataFrame is the SQLContext class (from Spark SQL).
 *  With a SQLContext, you can create DataFrames from:
 *  - an existing RDD
 *  - a Hive table
 *  - data sources...
 *
 *  In here we create a dataframe with the content of a JSON file.
 *
 */
public class PlayWithSparkDataFrame {
  final static Logger log = Logger.getLogger(PlayWithSparkDataFrame.class);

  // WARNING: Change the path
  // the path file where we stored the data
  private static String pathToFile = "file:///Users/ludwineprobst/DataSets/twitter/*";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("Spark DataFrames")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);

    // Create a sql context: the SQLContext wraps the SparkContext, and is specific to Spark SQL / Dataframe.
    // It is the entry point in Spark SQL.
    SQLContext sqlContext = new SQLContext(sc);

    // load the data (json file here) and register the data in the "tweets" table.
    DataFrame df = sqlContext.jsonFile(pathToFile);

    // Displays the content of the DataFrame to stdout
    //df.show();

    // we see something like that:
    // id         lang        name                 text
    // 632952234  en          Remembrance Day      Air Force Upgrade...

    // filter tweets in english and french
    DataFrame filtered = df.filter((df.col("lang").equalTo("en"))
                                        .or(df.col("lang").equalTo("fr")))
                           .toDF();

    filtered.show();
    // you should see something like that:
    // id         lang        name                 text
    // 2907381456 fr          sophie               springOne ça commence !

    // select the language
    df.select("lang").show();

    // Count the tweets for each language
    df.groupBy("lang").count().show();

    // another way using SQL queries
    // It will be use to determine the number of clusters we want for the K-means algorithm.
    df.registerTempTable("tweets");
    DataFrame result = sqlContext.sql("SELECT lang, COUNT(*) as cnt FROM tweets GROUP BY lang ORDER BY cnt DESC");

    System.out.println("number of languages: " + result.collectAsList().size());
    System.out.println(result.collectAsList());

    //df.registerTempTable("tweets");
    DataFrame reducedTweet = sqlContext.sql("SELECT lang, name, text FROM tweets");

    System.out.println("reduced Tweet: ");
    reducedTweet.show();
  }

}

