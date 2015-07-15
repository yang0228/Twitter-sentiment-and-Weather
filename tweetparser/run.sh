${SPARK_HOME}/bin/spark-submit  \
  --name "TweetCruncher" \
  --class au.org.aurin.parsing.TweetCruncher \
  --master "spark://vaneyck:7077" \
  --deploy-mode client \
   ./target/tweetcruncher-0.4.0-SNAPSHOT.jar \
  --instanceId tweeter \
  --zookeepers "localhost:2181" \
  --user root --password tweeter \
  --tableName tweet2 \
  --overwrite \
  --inputFile "hdfs://localhost:9000/byTimestamp.json" \
  --dictionaryFile "hdfs://localhost:9000/dictionary.txt"
