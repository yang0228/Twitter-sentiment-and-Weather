package au.org.aurin.parsing;

import java.util.Date;
import java.text.ParseException;
import org.joda.time.DateTime; 
import org.json.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import twitter4j.*;
import au.org.aurin.tweetcommons.Tweet;


/**
 * Class used to create Tweet JavaBean from Twitter4j Status
 * 
 * @author Qingyang Hong
 *
 */

public class TwitterFilter 
  implements Function<Status, Tweet> {

  private static final long serialVersionUID = 42l;

  @Override
  public Tweet call(Status status) {

    JSONObject json = new JSONObject();
    JSONObject value = new JSONObject();
    JSONArray key = new JSONArray();
    JSONArray location = new JSONArray();

    json.put("id", Long.toString(status.getId()));
    key.put(new DateTime(status.getCreatedAt()).toString());
    json.put("key", key);

    location.put(status.getGeoLocation().getLatitude());
    location.put(status.getGeoLocation().getLongitude());
    value.put("location", location);

    value.put("text", status.getText());
    value.put("sentiment",0.0);
    json.put("value", value);

    // String tweetJson = "{\"id\":\""+status.getId()+
    //     "\",\"key\":[\""+new DateTime(status.getCreatedAt())+
    //     "\"],\"value\":{\"id\":"+status.getId()+
    //     ",\"location\":["+status.getGeoLocation().getLatitude()+
    //     ","+status.getGeoLocation().getLongitude()+
    //     "],\"text\":\""+status.getText()+"\",\"sentiment\":0.0}}";

    try {
        return new Tweet(json);
    } catch (ParseException e) {
        return null;
    }    
  }
    
}
