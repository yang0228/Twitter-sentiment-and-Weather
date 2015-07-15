package au.org.aurin.parsing;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import java.util.*;
import twitter4j.*;
import twitter4j.json.DataObjectFactory;
import org.joda.time.DateTime;
import java.io.Serializable;
import org.json.JSONObject; 
import org.json.JSONArray;
import au.org.aurin.tweetcommons.Tweet;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Class used to test tweet processing from Twitter4j Status 
 * including, sentiment analysis and tokenizing operation
 * 
 * @author QingyangHong
 *
 */

public class SentimentTest implements Serializable {

    static private final JavaSparkContext ssc = 
      new JavaSparkContext(new SparkConf().setMaster("local[2]").setAppName("test"));

    static String ID = "597373424273526786";
    static String TIMESTAMP = "Sun May 10 12:11:37 UTC 2015";
    static Double LAT = -7.791350;
    static Double LONG = 110.353990;
    static String TEXT = "😊😊 (with KakArga at @kfcindonesia) — https://t.co/e5JwpqlQMI";
    static String[] TOKENS = {"kakarga", "https://t.co/e5jwpqlqmi", "@kfcindonesia"};
    static Double SENTIMENT = 2.0;
    
    private Status tweetStatus;    
    private List<Status> tweetList;   
    private JavaRDD<Status> rawTweet; 

    @Before
    public void setUp() {	
    }

    @Test
    public void sentimentScore() {
        try {

          JSONObject statusJson = new JSONObject(new String(readAllBytes(get(this.getClass().getResource("/statusJSON.json").toURI())))); 
          tweetStatus = DataObjectFactory.createStatus(statusJson.toString());
          tweetList = new ArrayList<Status>();
          tweetList.add(tweetStatus);
          rawTweet = ssc.parallelize(tweetList);

          JavaRDD<Tweet> sentiment = SentimentAnalysis.analyzeSentimentRDD(rawTweet);

          Tweet sampleTweet =  sentiment.collect().get(0);
          assertEquals(SentimentTest.ID, sampleTweet.getId());
          //System.out.println("TimeStamp->>>>>>>>>>>>>>>>>>>>>>>" + sampleTweet.getTimestamp().toDate().toString());
          assertEquals(SentimentTest.TIMESTAMP, sampleTweet.getTimestamp().toDate().toString());
          assertEquals(SentimentTest.LAT, sampleTweet.getLat(), 0.0001);
          assertEquals(SentimentTest.LONG, sampleTweet.getLon(), 0.001);
          assertEquals(SentimentTest.TEXT, sampleTweet.getText());
          assertEquals(SentimentTest.TOKENS.length, sampleTweet.getTokens().size());
          //System.out.println("Tokens->>>>>>>>>>>>>>>>>>>>>>>" + Arrays.toString(sampleTweet.getTokens().toArray()));
          for (int i = 0; i < TOKENS.length; i++){
              assertEquals(SentimentTest.TOKENS[i], sampleTweet.getTokens().toArray(new String[sampleTweet.getTokens().size()])[i]);
          }
          assertEquals(SentimentTest.SENTIMENT, sampleTweet.getSentiment());

          } catch (Exception e) {
            e.printStackTrace();
            fail();
          }

    }

    @After
    public void tearDown() {

	  ssc.stop();

    }
}
