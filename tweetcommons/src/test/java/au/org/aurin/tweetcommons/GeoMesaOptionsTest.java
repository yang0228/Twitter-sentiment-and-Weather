package au.org.aurin.tweetcommons;

import static org.junit.Assert.*;

import java.util.Map;

import org.kohsuke.args4j.CmdLineException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import au.org.aurin.tweetcommons.GeoMesaOptions;

public class GeoMesaOptionsTest {

  private GeoMesaOptions options;;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    this.options = new GeoMesaOptions();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void noArgsTest() throws CmdLineException, NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    String[] args = {};
    thrown.expect(CmdLineException.class);
    this.options.parse(args);
  }

  @Test
  public void noOverwriteArgTest() throws CmdLineException,
      NoSuchFieldException, SecurityException, IllegalArgumentException,
      IllegalAccessException {
    String[] args = { "--auths", "VAL", "--instanceId", "VAL", "--password",
        "VAL", "--tableName", "VAL", "--user", "VAL", "--zookeepers", "VAL" };
    this.options.parse(args);
    assertFalse(this.options.overwrite);
  }

  @Test
  public void overwriteArgTest() throws CmdLineException, NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    String[] args = { "--auths", "VAL", "--instanceId", "VAL", "--password",
        "VAL", "--tableName", "VAL", "--user", "VAL", "--zookeepers", "VAL",
        "--overwrite" };
    this.options.parse(args);
    assertTrue(this.options.overwrite);
  }

  @Test
  public void valueArgTest() throws CmdLineException, NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    String[] args = { "--auths", "VAL", "--instanceId", "VAL", "--password",
        "PASSWORD", "--tableName", "VAL", "--user", "VAL", "--zookeepers",
        "VAL" };
    this.options.parse(args);
    assertEquals("PASSWORD", this.options.password);
  }

  @Test
  public void mapArgTest() throws CmdLineException, NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    String[] args = { "--auths", "AUTHS", "--instanceId", "INSTANCEID",
        "--password", "PASSWORD", "--tableName", "TABLENAME", "--user", "USER",
        "--zookeepers", "ZOOKEEPERS" };
    this.options.parse(args);
    Map<String, String> values = this.options.getAccumuloOptions();
    assertEquals("AUTHS", values.get("auths"));
    assertEquals("INSTANCEID", values.get("instanceId"));
    assertEquals("PASSWORD", values.get("password"));
    assertEquals("TABLENAME", values.get("tableName"));
    assertEquals("USER", values.get("user"));
    assertEquals("ZOOKEEPERS", values.get("zookeepers"));
  }

}
