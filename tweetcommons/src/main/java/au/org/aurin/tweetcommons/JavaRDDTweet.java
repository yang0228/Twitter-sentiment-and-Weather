/**
 * 
 */
package au.org.aurin.tweetcommons;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.SchemaException;
import org.locationtech.geomesa.core.data.AccumuloFeatureStore;
import org.locationtech.geomesa.compute.spark.GeoMesaSpark;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import scala.runtime.AbstractFunction1;

/**
 * Class used as an helper for RDD composed of Tweets
 * 
 * @author lmorandini
 */
public class JavaRDDTweet {

  /**
   * Action that writes an RDD composed of JSON objects to a geo-enabled
   * Accumulo table TODO: at the appending/overwriting of an existing table
   *
   * @param rddIn
   *          RDD to write
   * @param options
   *          Parameters needed to create a feature store (passing a
   *          FeatureStore would not do, since it should point to a different
   *          feature store on every node)
   *
   * @return N. of features written
   * 
   * @throws IOException
   * @throws SchemaException
   */
  public static long saveToGeoMesaTable(JavaRDD<Tweet> rddIn,
      GeoMesaOptions options) throws IOException, SchemaException {

    // The following closure is executed once per partition
    JavaRDD<Integer> countFeatures = rddIn
        .mapPartitions((FlatMapFunction<Iterator<Tweet>, Integer>) (
            Iterator<Tweet> tweets) -> {

          // Builds a feature collection
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
        AccumuloFeatureStore featureSource = TweetFeatureStore
            .getFeatureType(options);
        Integer nFeatures = 0;

        // Adds all the features in the partition to the feature
        // collection
        while (tweets.hasNext()) {
          featureCollection.add(((Tweet) (tweets.next())).toSimpleFeature());
          nFeatures++;
        }

        // Writes the feature collection to GeoMesa
        featureSource.addFeatures(featureCollection);

        // Returns the number of features written
        Integer[] result = { nFeatures };
        return Arrays.asList(result);
      });

    // Triggers the evaluation and returns the number of features written
    long totFeatures = 0;
    for (Integer n : countFeatures.collect()) {
      totFeatures += n;
    }

    return totFeatures;
  }

  /**
   * Reads an RDD composed of Tweet objects from a geo-enabled Accumulo table
   * FIXME: this is just a work-around to pass integration tests, it will cause
   * OutOfMemory with sizable data
   *
   * @param sc
   *          Spark context
   * @param options
   *          Parameters needed to create a feature store (passing a
   *          FeatureStore would not do, since it should point to a different
   *          feature store on every node)
   *
   * @return A JavaRDD with the contents of the table
   * 
   * @throws IOException
   * @throws SchemaException
   */
  public static JavaRDD<Tweet> loadFromGeoMesaTable(JavaSparkContext sc,
      GeoMesaOptions options) throws IOException, SchemaException {

    List<Tweet> featList = new ArrayList<Tweet>();
    SimpleFeatureIterator featIter = TweetFeatureStore.getFeatureType(options)
        .getFeatures().features();
    try {
      while (featIter.hasNext()) {
        featList.add(new Tweet(featIter.next()));
      }
    } finally {
      featIter.close();
    }

    return sc.parallelize(Lists.newArrayList(featList));
  }

  /**
   * Reads an RDD composed of Tweet objects from a geo-enabled Accumulo table
   * FIXME: this does not work due to obscure interaction between Spark and
   * Scala (see thread "Compilaiton error in Java when applying "map
   * " to an RDD generated via GeoMesaSpark.rdd" on the geomesa-users list)
   *
   * @param conf
   *          Hadoop configuration
   * @param sc
   *          Spark context
   * @param options
   *          Parameters needed to create a feature store (passing a
   *          FeatureStore would not do, since it should point to a different
   *          feature store on every node)
   *
   * @return A JavaRDD with the contents of the table
   * 
   * @throws IOException
   * @throws SchemaException
   */
  @SuppressWarnings("unchecked")
  public static JavaRDD<Tweet> loadFromGeoMesaTable2(Configuration conf,
      JavaSparkContext sc, GeoMesaOptions options) throws IOException,
      SchemaException {

    @SuppressWarnings("rawtypes")
    scala.Function1 transform = new AbstractFunction1<SimpleFeature, Tweet>() {
      {
        System.out.println("*** 0 "); // XXX
      }

      public Tweet apply(SimpleFeature feature) {
        System.out.println("*** 1 " + feature.getID()); // XXX
        System.out.println("*** 2 " + (new Tweet(feature)).getId()); // XXX
        return new Tweet(feature);
      }
    };

    RDD<Object> rddOut = GeoMesaSpark.rdd(
        conf,
        sc.sc(),
        TweetFeatureStore.getFeatureType(options).getDataStore(),
        new Query(TweetFeatureStore.getFeatureType(options).featureName()
            .toString()), false).map(transform,
        scala.reflect.ClassTag$.MODULE$.apply(Tweet.class));

    return new JavaRDD(rddOut,
        scala.reflect.ClassTag$.MODULE$.apply(Tweet.class));
  }

  /**
   * Writes an JavaRDD<String> to an HDFS file, overwriting the file if it
   * exists. NODE: this should be in a helper class, since it applies to a wider
   * range than a JavaRDD made of tweets
   *
   * @param rddIn
   *          RDD to write
   * @param fileOut
   *          Name of file to write to
   */
  public static void saveRDDStringAsHDFS(JavaRDD<String> rddIn, String fileOut) {
    try {
      URI fileOutURI = new URI(fileOut);
      URI hdfsURI = new URI(fileOutURI.getScheme(), null, fileOutURI.getHost(),
          fileOutURI.getPort(), null, null, null);
      Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
      FileSystem hdfs = org.apache.hadoop.fs.FileSystem
          .get(hdfsURI, hadoopConf);
      hdfs.delete(new org.apache.hadoop.fs.Path(fileOut), true);
      rddIn.saveAsTextFile(fileOut);
    } catch (URISyntaxException | IOException e) {
      Logger.getRootLogger().error(e);
    }
  }

}