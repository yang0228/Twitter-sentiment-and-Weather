package au.org.aurin.tweetcommons;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/**
 * Class used to process the CLI interface options
 * 
 * @author lmorandini
 *
 */
public class CLIOptions {

  /**
   * Parses the options from a CLI array of strings. If a parsing exception is
   * thrown, prints error message and the usage on stderr, and re-throws the
   * exception.
   * 
   * @throws CmdLineException
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  public void parse(String[] args) throws CmdLineException,
      NoSuchFieldException, SecurityException, IllegalArgumentException,
      IllegalAccessException {
    CmdLineParser parser = new CmdLineParser(this);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      throw e;
    }
  }
}
