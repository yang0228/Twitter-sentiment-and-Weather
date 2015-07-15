package au.org.aurin.tweetcommons;

import java.io.IOException;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.locationtech.geomesa.core.data.AccumuloFeatureStore;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Class implemetinga GeoMesa feature store to hold WeatherItems
 * 
 * @author QingyangHong
 *
 */
public class WeatherFeatureStore {

  protected static String schemaDef = "location:Point:srid=4326,id:String,date:String,maxTemperature:Double,rainfall:Double,maxwindspeed:Double,mslp:Double";

  public WeatherFeatureStore() {
    // TODO Auto-generated constructor stub
  }

  /**
   * Returns a SimpleFeatureType to hold WeatherItems with tableName as name of table
   * 
   * @param tableName
   *          Accumulo table name for the schema
   * @throws SchemaException
   */
  public static SimpleFeatureType createSchema(String tableName)
      throws SchemaException {
    return DataUtilities.createType(tableName, WeatherFeatureStore.schemaDef);
  }

  /**
   * Returns the FeatureSource of name ftName (returns null if not existing)
   *
   * @param options
   *          Datastore parameters
   *
   * @return
   * @throws IOException
   * @throws SchemaException
   */
  public static AccumuloFeatureStore getFeatureType(GeoMesaOptions options)
      throws IOException, SchemaException {

    // Verifies that we can see this GeoMesa feature type
    DataStore dataStore = DataStoreFinder.getDataStore(options
        .getAccumuloOptions());
    if (dataStore == null) {
      return null;
    }

    // Creates the FeatureType if not existing
    if (dataStore.getSchema(options.tableName) == null) {
      return null;
    }

    return (AccumuloFeatureStore) dataStore.getFeatureSource(options.tableName);
  }

  /**
   * Creates if not existing a FeatureType in Accumulo, adn drops and re-creates
   * it if the overwrite options is set
   *
   * @param options
   *          Datastore parameters
   *
   * @return
   * @throws IOException
   * @throws SchemaException
   */
  public static void createFeatureType(GeoMesaOptions options)
      throws IOException, SchemaException {

    // Verifies that we can see this Accumulo feature type
    DataStore dataStore = DataStoreFinder.getDataStore(options
        .getAccumuloOptions());
    assert dataStore != null;

    // Drops the Feature Type if the overwrite options is set
    if (options.overwrite && dataStore.getSchema(options.tableName) != null) {
      dataStore.removeSchema(options.tableName);
    }

    // Creates the FeatureType if not existing
    if (dataStore.getSchema(options.tableName) == null) {
      dataStore.createSchema(WeatherFeatureStore.createSchema(options.tableName));
    }
  }
}
