package au.org.aurin.parsing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.geotools.feature.SchemaException;
import org.json.JSONException;
import org.json.JSONObject;
import org.kohsuke.args4j.CmdLineException;

import au.org.aurin.tweetcommons.JavaRDDTweet;
import au.org.aurin.tweetcommons.Tweet;
import au.org.aurin.tweetcommons.TweetFeatureStore;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

/**
 * @author lmorandini, QingyangHong
 * 
 *         Class used to read, parse, remove stopwords, and stem a Tweet
 *         expressed as a JSON object
 */
public class TweetCruncher {

  // CLI options
  private static TweetCruncherOptions options;

  // Members used for Stanford NLP
  private static Properties props = new Properties();
  private static List<String> keepPOS = (new ArrayList<String>());
  private static StanfordCoreNLP pipeline;

  // Initialization of Stanford NLP
  static {
    props.setProperty("annotators",
        "tokenize, ssplit, pos, lemma, parse, sentiment");
    props.setProperty("tokenize.whitespace", "false");
    props.setProperty("ssplit.tokenPatternsToDiscard", "allDelete");
    pipeline = new StanfordCoreNLP(TweetCruncher.props);

    /**
     * These are the Part of Speech to retain for topic modelling
     * 
     * @see https://catalog.ldc.upenn.edu/docs/LDC95T7/cl93.html
     * */
    keepPOS.addAll(Arrays.asList(new String[] { "FW", "JJ", "JJR", "JJS", "NN",
        "NNS", "NNP", "NNPS", "RB", "RP", "VB", "VBD", "VBG", "VBN", "VBP",
        "VBZ", "WRB" }));
  }

  /**
   * @param args
   *          The first
   * @throws SchemaException
   * @throws ParseException
   * @throws CmdLineException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   * @throws SecurityException
   * @throws NoSuchFieldException
   */
  public static void main(String[] args) throws IOException, SchemaException,
      ParseException, NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, CmdLineException {

    TweetCruncher.options = new TweetCruncherOptions();
    TweetCruncher.options.parse(args);

    // Processes the input file (the first argument in the command line)
    SparkConf conf = new SparkConf();

    final JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> rawTweets = sc.textFile(TweetCruncher.options.inputFile);

    // Filters out invalid JSON (first and last rows of the view returned by
    // CouchBase)
    // FIXME: time consuming, something better is needed
    JavaRDD<String> filteredTweets = rawTweets.filter((String inLine) -> {
      try {
        JSONObject obj = new JSONObject(inLine);
        obj.getString("id");
      } catch (JSONException e) {
        return false;
      }
      return true;
    });

    JavaRDD<Tweet> parsedTweets = filteredTweets.map((String inLine) -> {
      return TweetCruncher.processTweet(new Tweet(new JSONObject(inLine)));
    });

    JavaRDD<String> dict = TweetCruncher.createDictionary(parsedTweets);
    JavaRDDTweet.saveRDDStringAsHDFS(dict.distinct(),
        TweetCruncher.options.dictionaryFile);

    // Ensures the Accumulo Feature Type is created
    TweetFeatureStore.createFeatureType(TweetCruncher.options);

    // Save clean Tweets to Accumulo
    System.out.println("**** n. features written: "
        + JavaRDDTweet.saveToGeoMesaTable(parsedTweets, TweetCruncher.options));
  }

  /**
   * Parse and stem the Tweet text and returns the Tweet with an added "tokens"
   * property and sentiment computed
   * 
   * @see http://nlp.stanford.edu/sentiment/
   *
   * @param tweet
   *          Tweet to process
   * @return Processed tweet
   * @throws Exception
   */

  protected static Tweet processTweet(Tweet tweet) throws Exception {

    int mainSentiment = 0;
    int longest = 0;
    Annotation document = new Annotation(tweet.getText());

    // Runs all Annotators on this text
    TweetCruncher.pipeline.annotate(document);
    List<CoreMap> sentences = document.get(SentencesAnnotation.class);

    // Builds a dictionary made of all Parts of Speech contained in
    // TweetCruncher.keepPOS,
    // in passing, it computes the sentiment
    Set<String> lemmas = new HashSet<String>();
    String lemma;

    for (CoreMap sentence : sentences) {
      Tree tree = sentence.get(SentimentAnnotatedTree.class);
      int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
      String partText = sentence.toString();
      if (partText.length() > longest) {
         mainSentiment = sentiment;
         longest = partText.length();
      }
      for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
        String pos = token.get(PartOfSpeechAnnotation.class);
        if (TweetCruncher.keepPOS.contains(pos)) {
          lemma = token.lemma().toLowerCase();
          if (!lemmas.contains(lemma)) {
            lemmas.add(lemma);
          }
        }
      }
    }

    tweet.setSentiment(mainSentiment);
    tweet.getTokens().addAll(lemmas);
    return tweet;
  }

  /*
   * Creates a dictionary of tokens used in the tweets
   * 
   * @param parsedTweets RDD of input Tweets, with a tokens property containing
   * the stemmed words
   */
  protected static JavaRDD<String> createDictionary(JavaRDD<Tweet> parsedTweets) {

    // Pick up tokens from parsedTweets
    JavaRDD<String> words = parsedTweets.flatMap((t) -> {
      return t.getTokens();
    });

    return words;
  }

}