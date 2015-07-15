package au.org.aurin.parsing;

import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import twitter4j.*;
import twitter4j.json.DataObjectFactory;
import au.org.aurin.tweetcommons.Tweet;
import au.org.aurin.tweetcommons.JavaRDDTweet;
import au.org.aurin.parsing.*;
import java.util.*;


/**
 * Class harvest real-time tweet using Spark Twitter Stream API
 * and perform sentiment analysis and tokenizing operation
 * 
 * @author Qingyang Hong
 *
 */

public class SentimentAnalysis {

  /**
   * Map twitter4j Status from DStream to Tweet
   * property and sentiment computed
   * 
   *
   * @param Status
   *          twitter4j Status to process
   * @return Processed tweet with tokens and sentiment 
   * @throws Exception
   */

  public static JavaDStream<Tweet> analyzeSentimentDstream(JavaDStream<Status> filterGeo) {

    JavaDStream<Tweet> tweet = filterGeo.map (
        new TwitterFilter()
    );
    
    JavaDStream<Tweet> tweetTokens = tweet.map ((Tweet twt) -> {
      return TweetCruncher.processTweet(twt);
    });
    
    return tweetTokens;

  }

  /**
   * Map twitter4j Status from RDD to Tweet
   * property and sentiment computed
   * 
   *
   * @param Status
   *          twitter4j Status to process
   * @return Processed tweet with tokens and sentiment 
   * @throws Exception
   */
    
  public static JavaRDD<Tweet> analyzeSentimentRDD(JavaRDD<Status> filterGeo) {

    JavaRDD<Tweet> tweet = filterGeo.map (
        new TwitterFilter()
    );

    JavaRDD<Tweet> tweetTokens = tweet.map ((Tweet twt) -> {
      return TweetCruncher.processTweet(twt);
    });

    return tweetTokens;

  }


  public static void main(String[] args) {
        
    SparkConf conf = new SparkConf().
        setAppName("Twitter Stream Sentiment Analysis");

    if (args.length > 0)
        conf.setMaster(args[0]);
    else
        conf.setMaster("local[2]");

    JavaStreamingContext jssc = new JavaStreamingContext (
        conf, new Duration(10000));

    JavaDStream<Status> tweets = TwitterUtils.createStream(jssc);

    JavaDStream<Status> filterGeo = tweets.filter (
        new Function<Status, Boolean>() {

            private static final long serialVersionUID = 42l;
            @Override
            public Boolean call(Status tweet) {

        return tweet.getGeoLocation() != null;// && tweet.getLang().equals("en");
                       // && tweet.getPlace().getCountryCode().equals("AU");
        }
    });

    JavaDStream<Tweet> sentiment = SentimentAnalysis.analyzeSentimentDstream(filterGeo);

    JavaDStream<String> tweet_info = sentiment.map(
        new Function<Tweet, String>() {
            public String call(Tweet tweet) {

                    return "ID:"+tweet.getId()+
                    "\n Lat:"+tweet.getLat()+
                    "\n Longi:"+tweet.getLon()+
                    "\n date:"+tweet.getTimestamp()
                    +"\n text:"+tweet.getText()
                    +"\n tokens:"+Arrays.toString(tweet.getTokens().toArray())
                    +"\n sentiment:"+tweet.getSentiment().toString();
            }
        }

    );
    tweet_info.print();
    tweet_info.foreachRDD (
            new Function<JavaRDD<String>, Void>() {
                    @Override
                    public Void call(JavaRDD<String> rdd) throws Exception {

                            if(rdd!=null) {
                                    JavaRDDTweet.saveRDDStringAsHDFS(rdd,"TweetMelb");
                            }
                            return null;
                    }

           }
    );

    jssc.start();
    jssc.awaitTermination();
  }

  protected static void saveRDDAsHDFS(JavaRDD<String> tweet,
          String fileOut) {
    try {
      URI fileOutURI = new URI(fileOut);
      URI hdfsURI = new URI(fileOutURI.getScheme(), null,
              fileOutURI.getHost(), fileOutURI.getPort(), null, null,
              null);
      Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
      FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(hdfsURI,
              hadoopConf);
      //hdfs.delete(new org.apache.hadoop.fs.Path(fileOut), true);
      tweet.coalesce(1,true).saveAsTextFile(fileOut);
      //tweet.saveAsTextFile(fileOut);
    } catch (URISyntaxException | IOException e) {
      Logger.getRootLogger().error(e);
    }
  }
}
