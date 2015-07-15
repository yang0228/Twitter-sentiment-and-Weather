package au.org.aurin.parsing;

import org.kohsuke.args4j.Option;
import au.org.aurin.tweetcommons.GeoMesaOptions;

/**
 * Class used to process the CLI interface if TweetCruncher
 * 
 * @author lmorandini
 *
 */
public class TweetCruncherOptions extends GeoMesaOptions {

  // Options definitions
  @Option(name = "--inputFile", required = true, usage = "the HDFS file holding the tweet to process")
  public String inputFile;

  @Option(name = "--dictionaryFile", required = true, usage = "the output HDFS file holding the dictionary extracted from the Tweets")
  public String dictionaryFile;
}
