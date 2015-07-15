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
 * Class to test the marshalling/unmarshalling of a WeatherItem
 * 
 * @author QingyangHong
 *
 */
public class WeatherItemTest {

  static String WEATHERJSON = "{\"stationId\":\"086338\",\"key\":[\"2014-04-2\"],\"value\":{\"location\":[-37.8255,144.9816],\"maxTemperature\":24.9,\"rainfall\":0.0,\"maxwindspeed\":35,\"mslp\":1013.2}}";
  static String STATIONID = "086338";
  static String DATE = "2014-04-2";
  static Double LAT = -37.8255;
  static Double LON = 144.9816;
  static Double MAXTEMPERATURE = 24.9;
  static Double RAINFALL = 0.0;
  static Double MAXWINDSPEED = 35.0;
  static Double MSLP = 1013.2;

  private JSONObject json;

  @Before
  public void setUp() throws Exception {
    json = new JSONObject(WeatherItemTest.WEATHERJSON);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testWeatherJSON() {
    try {
      WeatherItem weather = new WeatherItem(json);
      assertEquals(WeatherItemTest.STATIONID, weather.getStationId());
      assertEquals(WeatherItemTest.DATE, weather.getDate());
      assertEquals(WeatherItemTest.LAT, weather.getLat(), 0.0001);
      assertEquals(WeatherItemTest.LON, weather.getLon(), 0.001);
      assertEquals(WeatherItemTest.MAXTEMPERATURE, weather.getMaxtemperature());
      assertEquals(WeatherItemTest.RAINFALL, weather.getRainfall());
      assertEquals(WeatherItemTest.MAXWINDSPEED, weather.getMaxwindspeed());
      assertEquals(WeatherItemTest.MSLP, weather.getMslp());

    } catch (JSONException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testFromSimpleFeature() throws SchemaException {
    try {
      SimpleFeature feat = (new WeatherItem(json)).toSimpleFeature();
      assertEquals(WeatherItemTest.STATIONID, feat.getID());
      assertEquals(WeatherItemTest.DATE,feat.getAttribute("date"));
      Geometry geom = (Geometry) feat.getAttribute("location");
      assertEquals(WeatherItemTest.LAT, geom.getCoordinate().y, 0.0001);
      assertEquals(WeatherItemTest.LON, geom.getCoordinate().x, 0.001);
      assertEquals(WeatherItemTest.MAXTEMPERATURE, feat.getAttribute("maxTemperature"));
      assertEquals(WeatherItemTest.RAINFALL, feat.getAttribute("rainfall"));
      assertEquals(WeatherItemTest.MAXWINDSPEED, feat.getAttribute("maxwindspeed"));
      assertEquals(WeatherItemTest.MSLP, feat.getAttribute("mslp"));

    } catch (JSONException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testToSimpleFeature() throws SchemaException {
    try {
      SimpleFeature feat = (new WeatherItem(json)).toSimpleFeature();
      WeatherItem weather = new WeatherItem(feat);
      assertEquals(WeatherItemTest.STATIONID, weather.getStationId());
      assertEquals(WeatherItemTest.DATE, weather.getDate());
      assertEquals(WeatherItemTest.LAT, weather.getLat(), 0.0001);
      assertEquals(WeatherItemTest.LON, weather.getLon(), 0.001);
      assertEquals(WeatherItemTest.MAXTEMPERATURE, weather.getMaxtemperature());
      assertEquals(WeatherItemTest.RAINFALL, weather.getRainfall());
      assertEquals(WeatherItemTest.MAXWINDSPEED, weather.getMaxwindspeed());
      assertEquals(WeatherItemTest.MSLP, weather.getMslp());
    } catch (JSONException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testToJSON() throws SchemaException {
    try {
      SimpleFeature feat = (new WeatherItem(json)).toSimpleFeature();
      WeatherItem weather = new WeatherItem(feat);
      JSONObject weatherToJSON = weather.toJSON();
      WeatherItem weatherFromJSON = new WeatherItem(weatherToJSON);
      assertEquals(weather.getStationId(), weatherFromJSON.getStationId());
      assertEquals(weather.getDate(), weatherFromJSON.getDate());
      assertEquals(weather.getLat(), weatherFromJSON.getLat());
      assertEquals(weather.getLon(), weatherFromJSON.getLon());
      assertEquals(weather.getMaxtemperature(), weatherFromJSON.getMaxtemperature());
      assertEquals(weather.getRainfall(), weatherFromJSON.getRainfall());
      assertEquals(weather.getMaxwindspeed(), weatherFromJSON.getMaxwindspeed());
      assertEquals(weather.getMslp(), weatherFromJSON.getMslp());
    } catch (JSONException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSerializaion() {
    File file = null;
    WeatherItem weatherOut = null;
    try {
      weatherOut = new WeatherItem(this.json);
      file = File.createTempFile("weather", ".ser");
      FileOutputStream fileOut = new FileOutputStream(file);
      ObjectOutputStream out = new ObjectOutputStream(fileOut);
      out.writeObject(weatherOut);
      out.close();
      fileOut.close();
    } catch (IOException | JSONException i) {
      i.printStackTrace();
      fail();
    }

    try {
      FileInputStream fileIn = new FileInputStream(file.getAbsoluteFile());
      ObjectInputStream in = new ObjectInputStream(fileIn);
      WeatherItem weatherIn = (WeatherItem) in.readObject();
      assertEquals(weatherOut.getStationId(), weatherIn.getStationId());
      in.close();
      fileIn.close();
    } catch (IOException | ClassNotFoundException i) {
      i.printStackTrace();
      fail();
    }
  }
}
