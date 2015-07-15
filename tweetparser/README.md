# tweetparser
A software to convert raw tweets into tokens, by parsing, removing stopwords, and stemming words.
The parse tweets are written, geo-referenced, in Accumulo/GeoMesa

## Installation steps

### Build the environment on your local machine

See thie Wiki page to install Hadoop, Spark, and Accumulo/GeoMesa:
http://wiki.aurin.org.au/display/TWIT/Stack+installation+procedure.

Start HDFS, Spark, and Accumulo.


### Load test data into HDFS

  `${HADOOP_HOME}/bin/hadoop fs -put ./data/byTimestamp.json`


### Build libstemmer

Download and build libstemmer following the instructions on http://snowball.tartarus.org/ 


### Build the project

  `mvn package -DskipTests=true`


### Run the project

  `${SPARK_HOME}/bin/spark-submit  \`
  `  --name "TweetCruncher" \`
  `  --class au.org.aurin.parsing.TweetCruncher \`
  `  --master "spark://vaneyck:7077" \`
  `  --deploy-mode client \`
  `  ./target/tweetcruncher-<version>.jar \`
  `  --instanceId tweeter \`
  `  --zookeepers "localhost:2181" \`
  `  --user root --password tweeter \`
  `  --tableName tweet2 \`
  `  --overwrite \`
  `  --inputFile "hdfs://localhost:9000/byTimestamp.json" \`
  `  --dictionaryFile "hdfs://localhost:9000/dictionary.txt"`


### Look at the results

  `${HADOOP_HOME}/bin/hadoop fs -cat hdfs://localhost:9000/dictionary.txt/*`

  `${ACCUMULO_HOME}/bin/bin/accumulo shell -u root`
  `scan -table tweet2_records`


### Run the unit tests

  `mvn test`


### Run the integration tests

- Start the Hadoop + Spark start
- Run the integrations tests:  `mvn integration-test`
