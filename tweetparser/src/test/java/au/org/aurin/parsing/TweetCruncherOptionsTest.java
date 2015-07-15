package au.org.aurin.parsing;

import org.kohsuke.args4j.Option;

import au.org.aurin.tweetcommons.CLIOptions;

/**
 * Class used to process the CLI interface if TweetCruncher
 * 
 * @author lmorandini
 *
 */
public class TweetCruncherOptionsTest extends CLIOptions {

  // Options definitions
  @Option(name = "--inputFile", required = true, usage = "the HDFS file holding the tweet to process")
  public String inputFile;
}
