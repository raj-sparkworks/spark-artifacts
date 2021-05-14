package com.util.parser

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, dense_rank,
  regexp_extract, substring, to_date}
import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.LogManager

object LogParser {

  private val LOGGER = LogManager.getLogger(LogParser.getClass)

  def main(args: Array[String]): Unit = {

    LOGGER.info("Loading the config file... ")

    /**
     * Load the env variable from the command and
     * parse the configurations
     */

    val envConfig = ConfigFactory.load().getConfig(args(0))

    val FILE_URL = envConfig.getString("file_url").trim
    val LOCAL_FILE_PATH = envConfig.getString("file_path").trim
    val THE_N_COUNT = envConfig.getString("n_count").trim
    val PULL_BAD_RECS = envConfig.getString("bad_records").trim
    val VISITOR_OUT_PATH = envConfig.getString("visitor_output").trim
    val URL_OUT_PATH = envConfig.getString("url_output").trim
    val BAD_REC_OUT_PATH = envConfig.getString("url_output").trim
    val theNCount = Integer.parseInt(THE_N_COUNT)

    val fileCode = FileUtil.initiateFileDownload(FILE_URL)

    if(fileCode == 0) {
      printf("Unable to download the file..")
      return
    }


    val spark = SparkSession.builder.
      appName("LogParser").
      getOrCreate()
    import spark.implicits._

    /**
     * If the input gz file having spittable file format
     * like JSON, XML, AVRO etc., then better decompress
     * the gz file for improved performance
     *
     * Since the input .gz contain txt file, giving as it is
     */
    val rawDF = spark.read.text(LOCAL_FILE_PATH)

    /**
     * Using regular expression to parse the log file
     * ([^(\s|,)]+) - Any alpha-numerical without
     * white space and comma
     * */
    val parsedDF = rawDF.select(
      regexp_extract($"value","""^([^(\s|,)]+)""",1).alias("Visitor"),
      regexp_extract($"value","""^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).as("Timestamp"),
      regexp_extract($"value","""^.*\w+\s+([^\s]+)\s+HTTP.*""",1).as("URL"))

    //If we need list of invalid/bad recs
    if("ON".equals(PULL_BAD_RECS.trim)) {
      val badRecDF = parsedDF.filter($"host".isNull
        || $"timestamp".isNull
        || $"path".isNull)

      outputStore(badRecDF,BAD_REC_OUT_PATH)
    }

    //Dropping the null records
    val cleanedDF = parsedDF.na.drop()

    //Convert the timestamp to Date for the below aggrigation
    val processedDF = cleanedDF.
      withColumn("date",
        to_date(substring($"timestamp",1,11), "dd/MMM/yyyy")).
      drop("timestamp")

    //Setting up the window spec
    val windowSpec = Window.
      partitionBy($"date").
      orderBy($"count".desc)

    val topVisitors = getTopFreqVisitors(
      processedDF,
      windowSpec,
      theNCount)

    val topURL = getTopFreqURL(
      processedDF,
      windowSpec,
      theNCount)

    println("Top Visitors : ")
    topVisitors.show(10,false)

    println("Top URL : ")
    topURL.show(10,false)

    println("Writing the output to file system...")
    outputStore(topVisitors,VISITOR_OUT_PATH)
    outputStore(topURL,URL_OUT_PATH)

    LOGGER.info("All Done !!!")

  }

  def getTopFreqVisitors(
                          processedDF : Dataset[Row],
                          windowSpec : WindowSpec,
                          theNCount : Int
                        ): Dataset[Row] =
  {
    val groupByVisitorDF = processedDF.
      groupBy("Visitor", "Date").
      count()

    val rankedVisitors = groupByVisitorDF.
      withColumn("TopVisitorsRank",
        dense_rank over windowSpec).
      filter(col("TopVisitorsRank") <= theNCount)
      //.orderBy(col("date").asc, col("TopVisitorsRank").desc)

    return rankedVisitors

  }

  def getTopFreqURL(
                     processedDF : Dataset[Row],
                     windowSpec : WindowSpec,
                     theNCount : Int
                   ): Dataset[Row] =
  {
    val groupByURLDF = processedDF.
      groupBy("URL", "Date").
      count()

    val rankedURL = groupByURLDF.
      withColumn("TopURLRank",
        dense_rank over windowSpec).
      filter(col("TopURLRank") <= theNCount)
      //.orderBy(col("date").asc, col("TopURLRank").desc)

    return rankedURL

  }

  //Function to write the results on FileSystem
  def outputStore(
                   theOutputDF: Dataset[Row],
                   theOutputPath: String) = {
   theOutputDF
      .write.option("header", "true")
      .mode("overwrite")
      .csv(theOutputPath)

    LOGGER.info("Output written to "+theOutputPath)
  }

}

