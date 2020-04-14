import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lower, udf, sum, mean, avg}
import scala.collection.mutable.WrappedArray
import scala.util.control.Breaks._

object YouTubeWithTweetsSentiment {

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: Please enter yesterday time: yyyy mm")
      System.exit(1)
    }
    
    val appName = "SentimentTweets"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[3]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val year = args(0);
    val month = args(1);
    
    import spark.implicits._
    val twitterDF = spark.read.load(s"hdfs://localhost:9000/datalake/preprocessed/twitter/sentiment/year=$year/month=$month")
    val sentimentDF = twitterDF.select("text", "sentiment")
    val youtubeDF = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/datalake/preprocessed/youtube/top100Videos.csv")
    val usYoutubeDF = youtubeDF.filter($"country" === "US")
    
    val checkValue = udf { 
      (tags: String, text: String) => {
        var join = false
        if (tags != null) {    
          val array = tags.split("\\|");
          breakable  
          {  
            for(tag <- array) {
              if (text.contains(tag)) {
                join = true
                break
              }
            }
          }
        }
        join
      }
    }
    
    val udfSentiment = udf((sentiment: Double) => {
      if (sentiment != null) {
        sentiment
      } else 0.0
    })
    val newDF = usYoutubeDF.join(sentimentDF, checkValue(usYoutubeDF("tags"), sentimentDF("text")) ,"left")
        .groupBy("country","channel_title","category","tags","publish_time","views","likes","dislikes","comment_count")
        .agg(udfSentiment(avg("sentiment")) as "sentimentAvg")
    
    newDF.write.format("parquet").save(s"hdfs://localhost:9000/datalake/preprocessed/twitter/youtube/year=$year/month=$month/youtubeWithTweetSentiment.parquet")
  }
}