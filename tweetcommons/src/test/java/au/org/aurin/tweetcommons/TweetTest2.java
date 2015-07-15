package au.org.aurin.tweetcommons;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;

import org.geotools.feature.SchemaException;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Class to test the marshalling/unmarshalling of a Tweet
 * 
 * @author lmorandini, QingyangHong
 *
 */

public class TweetTest2 {

  static String TWEETJSON = "{\"id\":\"509420203386499072\",\"key\":[\"2014-09-09T19:16:55.000Z\"],\"value\":{\"id\":509420203386499100,\"location\":[-35.20052195,149.14721482],\"text\":\"We aren't really ever having...\",\"sentiment\":0.25}}";
  static String TWEETJSONWITHTOKENS = "{\"id\":\"509420203386499072\",\"key\":[\"2014-09-09T19:16:55.000Z\"],\"value\":{\"id\":509420203386499100,\"location\":[-35.20052195,149.14721482],\"text\":\"We aren't really ever having...\", \"tokens\":[\"aaa\", \"bbb\"],\"sentiment\":0.25}}";
  static String TWEETJSONNOSENTIMENT = "{\"id\":\"509420203386499072\",\"key\":[\"2014-09-09T19:16:55.000Z\"],\"value\":{\"id\":509420203386499100,\"location\":[-35.20052195,149.14721482],\"text\":\"We aren't really ever having...\", \"tokens\":[\"aaa\", \"bbb\"]}}";
  static String TEXT = "We aren't really ever having...";
  static String ID = "509420203386499072";
  static String TIMESTAMP = "2014-09-09T19:16:55.000Z";
  static Double LAT = -35.20052195;
  static Double LON = 149.14721482;
  static String TOKEN1 = "aaa";
  static String TOKEN2 = "bbb";
  static Double SENTIMENT = 0.25;

  private JSONObject json, jsonWithTokens, jsonNoSentiment;

  @Before
  public void setUp() throws Exception {
    json = new JSONObject(TweetTest2.TWEETJSON);
    jsonWithTokens = new JSONObject(TweetTest2.TWEETJSONWITHTOKENS);
    jsonNoSentiment = new JSONObject(TweetTest2.TWEETJSONNOSENTIMENT);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testJSONWithoutTokens() {
    try {
      Tweet tweet = new Tweet(json);
      assertEquals(TweetTest2.ID, tweet.getId());
      assertTrue((new DateTime(TweetTest2.TIMESTAMP)).equals(tweet
          .getTimestamp()));
      assertEquals(TweetTest2.LAT, tweet.getLat(), 0.0001);
      assertEquals(TweetTest2.LON, tweet.getLon(), 0.001);
      assertEquals(TweetTest2.TEXT, tweet.getText());
      assertEquals(0, tweet.getTokens().size());
      tweet.getTokens().add("ccc");
      assertEquals(1, tweet.getTokens().size());
      assertEquals("ccc", tweet.getTokens().get(0));
      assertEquals(TweetTest2.SENTIMENT, tweet.getSentiment());
    } catch (JSONException | ParseException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testJSONWithTokens() {
    try {
      Tweet tweet = new Tweet(jsonWithTokens);
      assertEquals(2, tweet.getTokens().size());
      assertEquals(TweetTest2.TOKEN1, tweet.getTokens().get(0));
      assertEquals(TweetTest2.TOKEN2, tweet.getTokens().get(1));
    } catch (JSONException | ParseException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testJSONWithoutSentiment() {
    try {
      Tweet tweet = new Tweet(jsonNoSentiment);
      assertEquals(2, tweet.getTokens().size());
      assertEquals(TweetTest2.TOKEN1, tweet.getTokens().get(0));
      assertEquals(TweetTest2.TOKEN2, tweet.getTokens().get(1));
      assertEquals(0.0, tweet.getSentiment(), 1E-10);
    } catch (JSONException | ParseException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testFromSimpleFeature() throws SchemaException {
    try {
      SimpleFeature feat = (new Tweet(jsonWithTokens)).toSimpleFeature();
      assertEquals(TweetTest2.ID, feat.getID());
      assertTrue((new DateTime(TweetTest2.TIMESTAMP)).equals(new DateTime(feat
          .getAttribute("timestamp"))));
      Geometry geom = (Geometry) feat.getAttribute("location");
      assertEquals(TweetTest2.LAT, geom.getCoordinate().y, 0.0001);
      assertEquals(TweetTest2.LON, geom.getCoordinate().x, 0.001);
      assertEquals(TweetTest2.TEXT, feat.getAttribute("text"));
      JSONArray tokens = new JSONArray((String) feat.getAttribute("tokens"));
      assertEquals(2, tokens.length());
      assertEquals(TweetTest2.TOKEN1, tokens.getString(0));
      assertEquals(TweetTest2.TOKEN2, tokens.getString(1));
      assertEquals(TweetTest2.SENTIMENT.toString(), feat.getAttribute("sentiment"));
    } catch (JSONException | ParseException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testToSimpleFeature() throws SchemaException {
    try {
      SimpleFeature feat = (new Tweet(jsonWithTokens)).toSimpleFeature();
      Tweet tweet = new Tweet(feat);
      assertEquals(TweetTest2.ID, tweet.getId());
      assertTrue((new DateTime(TweetTest2.TIMESTAMP)).equals(tweet
          .getTimestamp()));
      assertEquals(TweetTest2.LAT, tweet.getLat(), 0.0001);
      assertEquals(TweetTest2.LON, tweet.getLon(), 0.001);
      assertEquals(TweetTest2.TEXT, tweet.getText());
      assertEquals(2, tweet.tokens.size());
      assertEquals(TweetTest2.TOKEN1, tweet.getTokens().get(0));
      assertEquals(TweetTest2.TOKEN2, tweet.getTokens().get(1));
      assertEquals(TweetTest2.SENTIMENT, tweet.getSentiment());
    } catch (JSONException | ParseException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testToJSON() throws SchemaException {
    try {
      SimpleFeature feat = (new Tweet(jsonWithTokens)).toSimpleFeature();
      Tweet tweet = new Tweet(feat);
      JSONObject json = tweet.toJSON();
      Tweet tweetFromJSON = new Tweet(json);
      assertEquals(tweet.getId(), tweetFromJSON.getId());
      assertEquals(tweet.getTimestamp(), tweetFromJSON.getTimestamp());
      assertEquals(tweet.getLat(), tweetFromJSON.getLat());
      assertEquals(tweet.getLon(), tweetFromJSON.getLon());
      assertEquals(tweet.getText(), tweetFromJSON.getText());
      assertEquals(tweet.getTokens().size(), tweetFromJSON.getTokens().size());
      assertEquals(tweet.getSentiment(), tweetFromJSON.getSentiment());
    } catch (JSONException | ParseException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSerializaion() {
    File file = null;
    Tweet tweetOut = null;
    try {
      tweetOut = new Tweet(this.jsonWithTokens);
      file = File.createTempFile("tweet", ".ser");
      FileOutputStream fileOut = new FileOutputStream(file);
      ObjectOutputStream out = new ObjectOutputStream(fileOut);
      out.writeObject(tweetOut);
      out.close();
      fileOut.close();
    } catch (IOException | JSONException | ParseException i) {
      i.printStackTrace();
      fail();
    }

    try {
      FileInputStream fileIn = new FileInputStream(file.getAbsoluteFile());
      ObjectInputStream in = new ObjectInputStream(fileIn);
      Tweet tweetIn = (Tweet) in.readObject();
      assertEquals(tweetOut.getId(), tweetIn.getId());
      in.close();
      fileIn.close();
    } catch (IOException | ClassNotFoundException i) {
      i.printStackTrace();
      fail();
    }
  }
}
