package au.org.aurin.tweetcommons;

import java.io.Serializable;

import org.geotools.factory.Hints;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.simple.SimpleFeature;
import com.vividsolutions.jts.geom.Geometry;


/**
 * Class implementing the marshalling/unmarshalling of 
 * a location-tagged weather observation
 *
 * @author QingyangHong
 *
 */
public class WeatherItem implements Serializable {

  private static final long serialVersionUID = 42l;

  protected String stationId;
  protected Double lat;
  protected Double lon;
  protected String date;
  protected Double maxTemperature;
  protected Double rainfall;
  protected Double maxwindspeed;
  protected Double mslp;


  // @formatter:off
  /**
   * Un-marshall the instance from the following JSON schema:
   *  {"stationId":"086338",
   *   "key":["2014-04-2"],
   *   "value: {
   *     "location" : [-37.8255,144.9816],
   *     "maxTemperature": 24.9,
   *     "rainfall": 0.0,
   *     "maxwindspeed": 35,
   *     "mslp": 1013.2
   *    }
   *  }
   *
   * @param json
   */
  // @formatter:on

  public WeatherItem(JSONObject json) throws JSONException {
    this.stationId = json.getString("stationId");
    this.date = json.getJSONArray("key").getString(0);
    this.lat = json.getJSONObject("value").getJSONArray("location")
        .getDouble(0);
    this.lon = json.getJSONObject("value").getJSONArray("location")
        .getDouble(1);
    this.maxTemperature = json.getJSONObject("value").getDouble("maxTemperature");
    this.rainfall = json.getJSONObject("value").getDouble("rainfall");
    this.maxwindspeed = json.getJSONObject("value").getDouble("maxwindspeed");
    this.mslp = json.getJSONObject("value").getDouble("mslp");

  }

  /**
   * Un-marshall the instance from a SimpleFeature
   *
   * @param feat
   */
  public WeatherItem(SimpleFeature feat) throws JSONException {
    this.stationId = feat.getID();
    this.date = (String) feat.getAttribute("date");
    Geometry geom = (Geometry) feat.getAttribute("location");
    this.lat = geom.getCoordinate().y;
    this.lon = geom.getCoordinate().x;
    this.maxTemperature = (Double) feat.getAttribute("maxTemperature");
    this.rainfall = (Double) feat.getAttribute("rainfall");
    this.maxwindspeed = (Double) feat.getAttribute("maxwindspeed");
    this.mslp = (Double) feat.getAttribute("mslp");
  }

  public JSONObject toJSON() {
    JSONObject json = new JSONObject();
    JSONObject value = new JSONObject();
    JSONArray key = new JSONArray();
    JSONArray location = new JSONArray();
    JSONArray tokens = new JSONArray();

    json.put("stationId", this.stationId);
    key.put(this.date);
    json.put("key", key);

    location.put(this.lat);
    location.put(this.lon);
    value.put("location", location);

    value.put("maxTemperature", this.maxTemperature);
    value.put("rainfall", this.rainfall);
    value.put("maxwindspeed", this.maxwindspeed);
    value.put("mslp", this.mslp);

    json.put("value", value);

    return json;
  }


  public SimpleFeature toSimpleFeature() throws SchemaException {

    // Creates a new feature
    SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
        WeatherFeatureStore.createSchema("weatherItem"));
    SimpleFeature simpleFeature = builder.buildFeature(this.stationId);
    simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID,
        java.lang.Boolean.TRUE);

    // Populates the new feature's attributes
    simpleFeature.setAttribute("location",
        WKTUtils$.MODULE$.read("POINT(" + this.lon + " " + this.lat + ")"));
    simpleFeature.setAttribute("date", this.date);
    simpleFeature.setAttribute("maxTemperature", this.maxTemperature);
    simpleFeature.setAttribute("rainfall", this.rainfall);    
    simpleFeature.setAttribute("maxwindspeed", this.maxwindspeed);
    simpleFeature.setAttribute("mslp", this.mslp);

    return simpleFeature;
  }

  /*
   * Selectors
   */
  public String getStationId() {
    return stationId;
  }


  public Double getLat() {
    return lat;
  }


  public Double getLon() {
    return lon;
  }


  public String getDate() {
    return date;
  }


  public Double getMaxtemperature() {
    return maxTemperature;
  }


  public Double getRainfall() {
    return rainfall;
  }


  public Double getMaxwindspeed() {
    return maxwindspeed;
  }

  public Double getMslp() {
    return mslp;
  }

  public void setStationId(String stationId) {
    this.stationId = stationId;
  }


  public void setLat(Double lat) {
    this.lat = lat;
  }


  public void setLon(Double lon) {
    this.lon = lon;
  }


  public void setDate(String date) {
    this.date = date;
  }


  public void setMaxtemperature(Double maxTemperature) {
    this.maxTemperature = maxTemperature;
  }


  public void setRainfall(Double rainfall) {
    this.rainfall = rainfall;
  }


  public void setMaxwindspeed(Double maxwindspeed) {
    this.maxwindspeed = maxwindspeed;
  }

  public void setMslp(Double mslp) {
    this.mslp = mslp;
  }

}
