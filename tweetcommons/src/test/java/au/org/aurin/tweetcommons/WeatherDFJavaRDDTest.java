package au.org.aurin.tweetcommons;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Column;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Class to test right RDDs holding WeatherItem from Spark Dataframe, 
 * which is created from source CSV files, are correct
 *
 * @author QingyangHong
 *
 */

public class WeatherDFJavaRDDTest {

  final JavaSparkContext sc = new JavaSparkContext((new SparkConf())
      .setAppName("WeatherDFJavaRDDTest").setMaster("local[2]"));
  final SQLContext sqlContext = new SQLContext(sc);
  final Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
  static DataFrame weatherDF;
  static DataFrame stationDF;

  @Before
  public void setUp() throws Exception {

    List<StructField> weatherfields = new ArrayList<StructField>();
    weatherfields.add(DataTypes.createStructField("StationId", DataTypes.StringType, false));
    weatherfields.add(DataTypes.createStructField("Date", DataTypes.StringType, false));
    weatherfields.add(DataTypes.createStructField("Minimum temperature (°C)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("Maximum temperature (°C)", DataTypes.DoubleType, false));
    weatherfields.add(DataTypes.createStructField("Rainfall (mm)", DataTypes.DoubleType, false));
    weatherfields.add(DataTypes.createStructField("Evaporation (mm)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("Sunshine (hours)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("Direction of maximum wind gust ", DataTypes.StringType, true));
    weatherfields.add(DataTypes.createStructField("Speed of maximum wind gust (km/h)", DataTypes.DoubleType, false));
    weatherfields.add(DataTypes.createStructField("Time of maximum wind gust", DataTypes.StringType, true));
    weatherfields.add(DataTypes.createStructField("9am Temperature (°C)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("9am relative humidity (%)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("9am cloud amount (oktas)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("9am wind direction", DataTypes.StringType, true));
    weatherfields.add(DataTypes.createStructField("9am wind speed (km/h)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("9am MSL pressure (hPa)", DataTypes.DoubleType, false));
    weatherfields.add(DataTypes.createStructField("3pm Temperature (°C)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("3pm relative humidity (%)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("3pm cloud amount (oktas)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("3pm wind direction", DataTypes.StringType, true));
    weatherfields.add(DataTypes.createStructField("3pm wind speed (km/h)", DataTypes.DoubleType, true));
    weatherfields.add(DataTypes.createStructField("3pm MSL pressure (hPa)", DataTypes.DoubleType, true));

    List<StructField> stationfields = new ArrayList<StructField>();
    stationfields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
    stationfields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
    stationfields.add(DataTypes.createStructField("lat", DataTypes.DoubleType, false));
    stationfields.add(DataTypes.createStructField("lon", DataTypes.DoubleType, false));

    StructType weatherSchema = DataTypes.createStructType(weatherfields);
    StructType stationSchema = DataTypes.createStructType(stationfields);

    HashMap<String, String> weatherOptions = new HashMap<String, String>();
    weatherOptions.put("header", "true");
    weatherOptions.put("path", this.getClass().getResource("/SampleIDCJDW3033.201404Refined.csv").toString());
    HashMap<String, String> stationOptions = new HashMap<String, String>();
    stationOptions.put("header", "true");
    stationOptions.put("path", this.getClass().getResource("/stationLatLon.csv").toString());

    weatherDF = sqlContext.load("com.databricks.spark.csv", weatherSchema, weatherOptions);
    stationDF = sqlContext.load("com.databricks.spark.csv", stationSchema, stationOptions);

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testDfToJavaRDD() {

    ArrayList<WeatherItem> weatherList = (new ArrayList<WeatherItem>());

    // Reads the test weatherItem dataframe 
    try {
      
      DataFrame weatherItemDF = weatherDF.join(stationDF, 
        weatherDF.col("StationId").equalTo(stationDF.col("id"))).select("StationId","lat","lon","Date",
        "Maximum temperature (°C)","Rainfall (mm)",
        "Speed of maximum wind gust (km/h)","9am MSL pressure (hPa)");
      //weatherItemDF.show();
      //System.out.println(weatherItemDF.schema().toString());

      JavaRDD<WeatherItem> weatherRDD = WeatherDFJavaRDD.DfToJavaRDD(weatherItemDF);
      weatherList = (ArrayList<WeatherItem>) weatherRDD.collect();
      assertEquals(2, weatherList.size());
      WeatherItem weatherItem = weatherList.get(1);
      assertEquals(WeatherItemTest.STATIONID, weatherItem.getStationId());
      assertEquals(WeatherItemTest.LAT, weatherItem.getLat(), 0.0001);
      assertEquals(WeatherItemTest.LON, weatherItem.getLon(), 0.001);
      assertEquals(WeatherItemTest.DATE, weatherItem.getDate());
      assertEquals(WeatherItemTest.MAXTEMPERATURE, weatherItem.getMaxtemperature());
      assertEquals(WeatherItemTest.RAINFALL, weatherItem.getRainfall());
      assertEquals(WeatherItemTest.MAXWINDSPEED, weatherItem.getMaxwindspeed());
      assertEquals(WeatherItemTest.MSLP, weatherItem.getMslp());
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
  }
}