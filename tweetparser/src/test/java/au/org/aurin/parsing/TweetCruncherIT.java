package au.org.aurin.parsing;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;
import au.org.aurin.tweetcommons.Tweet;

public class TweetCruncherIT {

  static private TweetCruncherOptionsTest options;
  static private String[] args = System.getProperty("geomesa-args").split(
      "\\s+");

  static private final JavaSparkContext sc = new JavaSparkContext((new SparkConf())
      .setAppName("TweetCruncherIT").setMaster("local[2]"));
  static private final Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

  static {
    TweetCruncherIT.options = new TweetCruncherOptionsTest();
    try {
      TweetCruncherIT.options.parse(args);
    } catch (NoSuchFieldException | SecurityException
        | IllegalArgumentException | IllegalAccessException | CmdLineException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testParsingOfTweets() {

    SparkConf conf = new SparkConf();

    JavaRDD<String> rawTweets = TweetCruncherIT.sc.textFile(TweetCruncherIT.options.inputFile);

    JavaRDD<String> filteredTweets = rawTweets.filter((String inLine) -> {
      try {
        JSONObject obj = new JSONObject(inLine);
        obj.getString("id");
      } catch (JSONException e) {
        return false;
      }
      return true;
    });

    JavaRDD<Tweet> parsedTweets = filteredTweets.map((String inLine) -> {
      return TweetCruncher.processTweet(new Tweet(new JSONObject(inLine)));
    });

    JavaRDD<String> dict = parsedTweets.flatMap((t) -> {
        return t.getTokens();
      });

      assertEquals(728, dict.collect().size());
  }
}
