package au.org.aurin.tweetcommons;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.BooleanOptionHandler;

/**
 * Class used to process the CLI interface options specific to GeoMesa
 * 
 * @author lmorandini
 *
 */
public class GeoMesaOptions extends CLIOptions implements Serializable {

  // Options definitions
  @Option(name = "--instanceId", required = true, usage = "the ID (name) of the Accumulo instance, e.g:  mycloud")
  public String instanceId;

  @Option(name = "--zookeepers", required = true, usage = "the comma-separated list of Zookeeper nodes that support your Accumulo instance, e.g.:  zoo1:2181,zoo2:2181,zoo3:2181")
  public String zookeepers;

  @Option(name = "--user", required = true, usage = "he Accumulo user that will own the connection, e.g.:  root")
  public String user;

  @Option(name = "--password", required = true, usage = "the password for the Accumulo user that will own the connection, e.g.:  thor")
  public String password;

  @Option(name = "--auths", required = false, usage = "the (optional) list of comma-separated Accumulo authorizations that should be applied to all data written or read by this Accumulo user; note that this is NOT the list of low-level database permissions such as 'Table.READ', but more a series of text tokens that decorate cell data, e.g.:  Accounting,Purchasing,Testing")
  public String auths;

  @Option(name = "--tableName", required = true, usage = "the name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data")
  public String tableName;

  @Option(name = "--overwrite", required = false, handler = BooleanOptionHandler.class, usage = "use this option if you want to overwrite the existing data in the table, default is true")
  public boolean overwrite;

  /**
   * Returns a Map with all name and values of the Accumulo options (used by
   * GeoMesa to access the database)
   * 
   * @return
   */
  public Map<String, String> getAccumuloOptions() {
    Map<String, String> values = new HashMap<String, String>();
    values.put("instanceId", this.instanceId);
    values.put("zookeepers", this.zookeepers);
    values.put("user", this.user);
    values.put("password", this.password);
    values.put("auths", this.auths);
    values.put("tableName", this.tableName);
    return values;
  }
}
