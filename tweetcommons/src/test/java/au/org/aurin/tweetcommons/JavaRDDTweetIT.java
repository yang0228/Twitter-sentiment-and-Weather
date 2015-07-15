package au.org.aurin.tweetcommons;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.SchemaException;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.core.data.AccumuloFeatureStore;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

public class JavaRDDTweetIT {

  static private GeoMesaOptions options;
  static private String[] args = System.getProperty("geomesa-args").split(
      "\\s+");

  final JavaSparkContext sc = new JavaSparkContext((new SparkConf())
      .setAppName("JavRDDTweetIT").setMaster("local[2]"));
  final Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

  static {
    JavaRDDTweetIT.options = new GeoMesaOptions();
    try {
      JavaRDDTweetIT.options.parse(args);
    } catch (NoSuchFieldException | SecurityException
        | IllegalArgumentException | IllegalAccessException | CmdLineException e) {
      e.printStackTrace();
      fail();
    }
  }

  private JSONObject json, jsonWithTokens;

  @Before
  public void setUp() throws Exception {
    json = new JSONObject(TweetTest2.TWEETJSON);
    jsonWithTokens = new JSONObject(TweetTest2.TWEETJSONWITHTOKENS);

    if (TweetFeatureStore.getFeatureType(JavaRDDTweetIT.options) != null) {
      TweetFeatureStore.getFeatureType(JavaRDDTweetIT.options).dataStore()
          .delete();
    }

    try {
      TweetFeatureStore.createFeatureType(JavaRDDTweetIT.options);
    } catch (IOException | SchemaException e) {
      e.printStackTrace();
      fail();
    }
  }

  @After
  public void tearDown() throws Exception {
    TweetFeatureStore.getFeatureType(JavaRDDTweetIT.options).dataStore()
        .delete();
  }

  @Test
  public void testTransaction() {
    AccumuloFeatureStore ftype = null;
    ArrayList<Tweet> tweetList = (new ArrayList<Tweet>());

    try {
      ftype = TweetFeatureStore.getFeatureType(JavaRDDTweetIT.options);
    } catch (IOException | SchemaException e) {
      e.printStackTrace();
      fail();
    }

    // Writes the test Tweet
    try {
      tweetList.add(new Tweet(jsonWithTokens));
      JavaRDDTweet.saveToGeoMesaTable(sc.parallelize(tweetList),
          JavaRDDTweetIT.options);
    } catch (IOException | SchemaException | JSONException | ParseException e) {
      e.printStackTrace();
      fail();
    }

    // Reads the test Tweet
    try {
      JavaRDD<Tweet> tweets = JavaRDDTweet.loadFromGeoMesaTable(sc,
          JavaRDDTweetIT.options);
      tweetList = (ArrayList<Tweet>) tweets.collect();
      assertEquals(1, tweetList.size());
      Tweet tweet = tweetList.get(0);
      assertEquals(TweetTest2.ID, tweet.getId());
      assertTrue((new DateTime(TweetTest2.TIMESTAMP)).equals(new DateTime(tweet.getTimestamp())));
      assertEquals(TweetTest2.LAT, tweet.getLat(), 0.0001);
      assertEquals(TweetTest2.LON, tweet.getLon(), 0.001);
      assertEquals(TweetTest2.TEXT, tweet.getText());
      List<String> tokens = tweet.getTokens();
      assertEquals(2, tokens.size());
      assertEquals(TweetTest2.TOKEN1, tokens.get(0));
      assertEquals(TweetTest2.TOKEN2, tokens.get(1));
      assertEquals(TweetTest2.SENTIMENT, tweet.getSentiment());
    } catch (IOException | SchemaException | JSONException e) {
      e.printStackTrace();
      fail();
    }
  }
}