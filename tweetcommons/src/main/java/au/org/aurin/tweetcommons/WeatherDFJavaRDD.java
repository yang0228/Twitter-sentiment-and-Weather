package au.org.aurin.tweetcommons;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

/**
 * Class used to marshal RDD of WeatherItems from Spark Dataframe 
 * 
 * @author QingyangHong
 */
public class WeatherDFJavaRDD {

  /**
   * Action that create WeatherItem rdd from weather Spark Dataframe 
   *
   * @param weatherDF
   *          weather Spark Dataframe with schema:
   *          |-- id: string (nullable = false)
   *          |-- lat: double (nullable = false)
   *          |-- lon: double (nullable = false)
   *          |-- Date: string (nullable = false)
   *          |-- Maximum temperature (°C): double (nullable = false)
   *          |-- Rainfall (mm): double (nullable = false)
   *          |-- Speed of maximum wind gust (km/h): double (nullable = false)
   *          |-- 9am MSL pressure (hPa): double (nullable = false)
   * @return JavaRDD of WeatherItem
   * 
   * @throws IOException
   */
  public static JavaRDD<WeatherItem> DfToJavaRDD(DataFrame weatherDF) 
      throws IOException {

    JavaRDD<WeatherItem> weatherJavaRDD = weatherDF.javaRDD().map(
        new Function<Row, WeatherItem>() {

          private static final long serialVersionUID = 42l;
          @Override
          public WeatherItem call(Row row) {

            JSONObject json = new JSONObject();
            JSONObject value = new JSONObject();
            JSONArray key = new JSONArray();
            JSONArray location = new JSONArray();
    
            json.put("stationId", row.getString(0));
            key.put(row.getString(3));
            json.put("key", key);
    
            location.put(row.getDouble(1));
            location.put(row.getDouble(2));
            value.put("location", location);
    
            value.put("maxTemperature", row.getDouble(4));
            value.put("rainfall", row.getDouble(5));
            value.put("maxwindspeed", row.getDouble(6));
            value.put("mslp", row.getDouble(7));
    
            json.put("value", value);
            
            return new WeatherItem(json);
          }

        });

    return weatherJavaRDD;
  }

}