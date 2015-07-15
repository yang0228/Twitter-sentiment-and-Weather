package au.org.aurin.parsing;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import au.org.aurin.tweetcommons.Tweet;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class ParsingTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testParsing() throws Exception {
    Tweet tweet = new Tweet(
        new JSONObject(
            "{\"id\":\"509420203386499072\",\"key\":[\"2014-09-09T19:16:55.000Z\"],"
                + "\"value\":{\"id\":509420203386499100,\"location\":[-35.20052195,149.14721482],\"text\":"
                + "\"Better later than never\",\"sentiment\":2.0}}"));
    TweetCruncher.processTweet(tweet);
    assertEquals(3, tweet.getTokens().size());
  }

  @Test
  public void testParsingWithStemming() throws Exception {
    Tweet tweet = new Tweet(
        new JSONObject(
            "{\"id\":\"509420203386499072\",\"key\":[\"2014-09-09T19:16:55.000Z\"],"
                + "\"value\":{\"id\":509420203386499100,\"location\":[-35.20052195,149.14721482],\"text\":"
                + "\"The central office closes regional Offices \",\"sentiment\":2.0}}"));
    TweetCruncher.processTweet(tweet);
    assertEquals(4, tweet.getTokens().size());
  }

  public void testParsingMultipleTweets() throws Exception {
    Tweet tweet1 = new Tweet(
        new JSONObject(
            "{\"id\":\"509420203386499072\",\"key\":[\"2014-09-09T19:16:55.000Z\"],"
                + "\"value\":{\"id\":509420203386499100,\"location\":[-35.20052195,149.14721482],\"text\":"
                + "\"The central office closes regional offices \",\"sentiment\":2.0}}"));
    Tweet tweet2 = new Tweet(
        new JSONObject(
            "{\"id\":\"509420203386499072\",\"key\":[\"2014-09-09T19:16:55.000Z\"],"
                + "\"value\":{\"id\":509420203386499100,\"location\":[-35.20052195,149.14721482],\"text\":"
                + "\"Office of Strategic Influence\",\"sentiment\":2.0}}"));
    TweetCruncher.processTweet(tweet1);
    TweetCruncher.processTweet(tweet2);
    assertEquals(4, tweet1.getTokens().size());
    assertEquals(3, tweet2.getTokens().size());
  }

  /*
   * TODO public void testSeniment() throws Exception { Tweet tweet = new Tweet(
   * new JSONObject(
   * "{\"id\":\"509420203386499072\",\"key\":[\"2014-09-09T19:16:55.000Z\"]," +
   * "\"value\":{\"id\":509420203386499100,\"location\":[-35.20052195,149.14721482],\"text\":"
   * + "\"I am feeling miserable\"}}")); TweetCruncher.processTweet(tweet);
   * assertEquals(2, tweet.getSentiment()); }
   */
}
