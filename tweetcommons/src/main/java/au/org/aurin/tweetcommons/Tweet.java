package au.org.aurin.tweetcommons;

import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.geotools.factory.Hints;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.simple.SimpleFeature;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Class implementing the marshalling/unmarshalling of a Tweet
 * 
 * @author lmorandini, QingyangHong
 *
 */
public class Tweet implements Serializable {

  private static final long serialVersionUID = 2602204312429599716L;

  protected String id;
  protected Double lat;
  protected Double lon;
  protected DateTime timestamp;
  protected String text;
  protected List<String> tokens;
  protected Double sentiment;

  // @formatter:off
  /**
   * Un-marshall the instance from the following JSON schema:
   *  {"id":"560693832614813697",
   *   "timestamp":"2015-01-29T07:00:01.000Z",
   *   "value: {
   *     "location" : [35.213318, 149.08920988],
   *     "text": "no wonder the flying foxes",
   *     "tokens":"rspcaqld fli fox amp risk ",
   *     "sentiment": 2.0
   *    }
   *  }
   *
   * @param json
   * @throws ParseException
   * @throws JSONException
   */
  // @formatter:on
  public Tweet(JSONObject json) throws JSONException, ParseException {
    this.id = json.getString("id");
    this.timestamp = new DateTime(json.getJSONArray("key").getString(0));
    this.lat = json.getJSONObject("value").getJSONArray("location")
        .getDouble(0);
    this.lon = json.getJSONObject("value").getJSONArray("location")
        .getDouble(1);
    this.text = json.getJSONObject("value").getString("text");

    // Tokens may not exist, and we do not want it to fail
    JSONArray tweetTokens;
    try {
      tweetTokens = json.getJSONObject("value").getJSONArray("tokens");
      this.tokens = new ArrayList<String>(tweetTokens.length());
      for (int i = 0; i < tweetTokens.length(); i++) {
        this.tokens.add(tweetTokens.getString(i));
      }
    } catch (JSONException e) {
      if (e.getMessage().contains("not found")) {
        this.tokens = new ArrayList<String>(0);
      } else {
        throw e;
      }

    }

    // Sentiment may not exist, and we do not want it to fail
    try {
      this.sentiment = json.getJSONObject("value").getDouble("sentiment");
    } catch (JSONException e) {
      if (e.getMessage().contains("not found")) {
        this.sentiment = 0.0;
      } else {
        throw e;
      }
    }
  }

  /**
   * Un-marshall the instance from a SimpleFeature
   *
   * @param feat
   */
  public Tweet(SimpleFeature feat) throws JSONException {
    this.id = feat.getID();
    this.timestamp = new DateTime(feat.getAttribute("timestamp"));
    Geometry geom = (Geometry) feat.getAttribute("location");
    this.lat = geom.getCoordinate().y;
    this.lon = geom.getCoordinate().x;
    this.text = (String) feat.getAttribute("text");

    // Tokens may not exist, and we do not want it to fail
    JSONArray tweetTokens;
    try {
      tweetTokens = new JSONArray((String) feat.getAttribute("tokens"));
      this.tokens = new ArrayList<String>(tweetTokens.length());
      for (int i = 0; i < tweetTokens.length(); i++) {
        this.tokens.add(tweetTokens.getString(i));
      }
    } catch (JSONException e) {
      if (e.getMessage().contains("not found")) {
        this.tokens = new ArrayList<String>(0);
      } else {
        throw e;
      }
    }

    // Sentiment may not exist, and we do not want it to fail
    try {
      this.sentiment = Double.parseDouble((String) feat.getAttribute("sentiment"));
    } catch (JSONException e) {
      if (e.getMessage().contains("not found")) {
        this.sentiment = 0.0;
      } else {
        throw e;
      }
    }
  }

  /**
   * Returns the Tweet marshalled in SimpleFeature according to the given schema
   * 
   * @param schema
   *          Schema of the feature type
   * @return
   * @throws SchemaException
   */
  public SimpleFeature toSimpleFeature() throws SchemaException {

    // Creates a new feature
    SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
        TweetFeatureStore.createSchema("tweet"));
    SimpleFeature simpleFeature = builder.buildFeature(this.id);
    simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID,
        java.lang.Boolean.TRUE);

    // Populates the new feature's attributes
    simpleFeature.setAttribute("location",
        WKTUtils$.MODULE$.read("POINT(" + this.lon + " " + this.lat + ")"));
    simpleFeature.setAttribute("timestamp", this.timestamp.toString());
    simpleFeature.setAttribute("text", this.text);
    simpleFeature.setAttribute("tokens",
        (new JSONArray(this.tokens.toArray())).toString());
    simpleFeature.setAttribute("sentiment", this.sentiment.toString());    

    return simpleFeature;
  }

  /**
   * Returns the Tweet marshalled in a JSONObject, using the same schema as the
   * Tweet(JSONObject) constructor
   * 
   * @param schema
   *          Schema of the feature type
   * @return
   */
  public JSONObject toJSON() {
    JSONObject json = new JSONObject();
    JSONObject value = new JSONObject();
    JSONArray key = new JSONArray();
    JSONArray location = new JSONArray();
    JSONArray tokens = new JSONArray();

    json.put("id", this.id);
    key.put(this.timestamp.toString());
    json.put("key", key);

    location.put(this.lat);
    location.put(this.lon);
    value.put("location", location);

    value.put("text", this.text);

    for (String token : this.tokens) {
      tokens.put(token);
    }

    value.put("tokens",tokens);
    value.put("sentiment", this.sentiment);    
    json.put("value", value);

    return json;
  }

  /*
   * Selectors
   */
  public String getId() {
    return id;
  }

  public Double getLat() {
    return lat;
  }

  public Double getLon() {
    return lon;
  }

  public DateTime getTimestamp() {
    return timestamp;
  }

  public String getText() {
    return text;
  }

  public List<String> getTokens() {
    return tokens;
  }

  public Double getSentiment() {
    return sentiment;
  }

  public void setSentiment(double sentiment) {
    this.sentiment = sentiment;
  }
}
