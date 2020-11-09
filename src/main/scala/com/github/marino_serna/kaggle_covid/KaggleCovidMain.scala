package com.github.marino_serna.kaggle_covid

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.johnsnowlabs.nlp.annotator.LanguageDetectorDL
import com.johnsnowlabs.nlp.base.DocumentAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.input_file_name
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.johnsnowlabs.nlp.embeddings.UniversalSentenceEncoder
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession
//import org.elasticsearch.spark.sql._

import org.graphframes._

object KaggleCovidMain {

  def main(args: Array[String]): Unit = {
    println(args)

    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val path = args(0)

    kaggleETL(ss: SparkSession, path:String)
  }

  def kaggleETL(ss: SparkSession, path:String)={
    val dfRaw = readRawData(ss, path)

    val dfFilterLang = filterEnglish(ss: SparkSession, dfRaw:DataFrame, path)

    val timestampST = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
    val fileName = s"$path/har/kaggles_$timestampST.parquet"
    writeParquet(dfFilterLang, fileName)


    val dfLoaded = readParquet(ss, fileName)
    dfLoaded.show()

    val dfCleanWords = stopWords(ss, dfLoaded)
    dfCleanWords.show(20, false)
    writeElasticSearch(dfCleanWords, "kaggle/abstract")

    val dfFrequency = frequentWord(dfCleanWords,"abstract","topWord")
    dfFrequency.show()

    val gf = authorGraph(ss, dfRaw)

    val dfRankedAuthors = degreeCentrality(gf)
    val rankedAuthorsFileName = s"$path/har/rankedAuthors_$timestampST.parquet"
    writeParquet(dfRankedAuthors, rankedAuthorsFileName)

    val rankedSimilarity = compareDocumentSimilarity(ss, dfRaw, "000a0fc8bbef80410199e690191dc3076a290117")

    val rankFileName = s"$path/har/rankedSimilarity_$timestampST.parquet"
    writeParquet(rankedSimilarity, rankFileName)

    moveFiles(dfFilterLang,path, "processed")
  }

  def moveFiles(df:DataFrame, path:String, folder:String) ={
    df.show(10)
    df.select("filename").foreach(x => {
      val inputFile:String = x.get(0).toString
      val outputFile = inputFile.replace("/input/",s"/$folder/")
      if(inputFile.startsWith("dbfs:")){
        dbutils.fs.mv(inputFile,outputFile)
      }else{ // unit tests
        println(s" $folder => move file from $inputFile to $outputFile")
      }
    })
  }

  def readRawData(ss: SparkSession, path:String):DataFrame ={
    val df = ss.read.option("multiline", "true").json(s"${path}/input/*")
      .withColumn("filename", input_file_name)

    if(df.schema.fields.map(_.name).contains("_corrupt_record")){
      moveFiles(df.filter("_corrupt_record is not null"),path, "error")
      df.filter("_corrupt_record is null").drop("_corrupt_record")
    }else{
      df
    }
  }

  def filterEnglish(ss: SparkSession, df:DataFrame, path:String):DataFrame = {
    import ss.implicits._

    val dfBody = df.withColumn("langDocument", $"body_text.text".cast(StringType))

    val model_name = "ld_wiki_20"

    val documentAssembler = new DocumentAssembler()
      .setInputCol("langDocument")
      .setOutputCol("document")

    val langDet = LanguageDetectorDL
      .pretrained(model_name)
      .setInputCols("document")
      .setOutputCol("language")
      .setCoalesceSentences(true)
      .setThreshold(0.5F)

    val pipeline = new Pipeline()
      .setStages(
        Array(
          documentAssembler,
          langDet
        )
      )

    val pipelineModel = pipeline.fit(dfBody)
    val pipelineDF = pipelineModel.transform(dfBody)

    val dfLang = pipelineDF.withColumn("language", $"language.result".cast(StringType))

    moveFiles(dfLang.filter($"language" =!= "[en]"),path, "error")

    dfLang
      .filter($"language" === "[en]")
      .drop("langDocument", "language")
  }

  def writeParquet(df:DataFrame, path:String) = {
    df.repartition(1).write.mode(SaveMode.Overwrite).parquet(path)
  }

  def readParquet(ss: SparkSession, path:String):DataFrame = {
    ss.read.parquet(path)
  }

  def stopWords(ss: SparkSession, df:DataFrame):DataFrame ={
    import ss.implicits._

    StopWordsRemover.loadDefaultStopWords("english")

    val remover = new StopWordsRemover()
      .setInputCol("abstractToClean")
      .setOutputCol("cleanAbstract")

    val cleanDF = remover.transform(
      df.withColumn("abstractToClean",
        split(
          regexp_replace($"abstract.text".cast(StringType), "[^A-z0-9_\\s]", "")
          ," ")))

    cleanDF
      .withColumn("abstract", $"cleanAbstract")
      .select("paper_id","abstract")
  }

  def writeElasticSearch(df:DataFrame, table:String) ={
//    df.saveToEs(table)
  }

  def frequentWord(df:DataFrame, inputColumn:String, outputColumn:String):DataFrame = {
    df
      .select(col("paper_id"), explode(col(inputColumn)).as(outputColumn))
      .groupBy("paper_id", outputColumn)
      .agg(count("*").as("amount"))
      .orderBy(desc("amount"))
      .groupBy("paper_id")
      .agg(
        first("amount").as("maxAmount"),
        first(outputColumn).as(outputColumn)
      )
  }

  def authorGraph(ss: SparkSession, df:DataFrame):GraphFrame ={
    import ss.implicits._
    val dfData = df
      .withColumn("authors", $"metadata.authors")
      .select($"paper_id".cast(StringType), explode($"authors").as("authors"))
      .withColumn("authorID", concat_ws("_",$"authors.first", $"authors.middle".cast(StringType), $"authors.last", $"authors.suffix"))

    val dfAuthors = dfData.select($"authorID".as("id"), $"authors").distinct()

    val dfDocumentsSrc = dfData
      .select($"paper_id", $"authorID".as("src")).distinct()

    val dfDocumentsDst = dfDocumentsSrc
      .withColumnRenamed("src","dst")

      val dfDocuments = dfDocumentsDst
        .join(dfDocumentsSrc, dfDocumentsSrc("paper_id") === dfDocumentsDst("paper_id"), "left")
        .drop(dfDocumentsDst("paper_id"))
        .filter($"src" =!= $"dst") // The graph is only for co-authorship, so must be at least 2 authors to be consider
        .distinct()

    GraphFrame(dfAuthors, dfDocuments)

  }

  def degreeCentrality(gf:GraphFrame):DataFrame ={

    gf.inDegrees.orderBy(desc("inDegree"))

  }

  def compareDocumentSimilarity(ss: SparkSession, df:DataFrame, paperId:String):DataFrame ={
    import ss.implicits._

    val dfBody = df.withColumn("similarityDocument", $"abstract.text".cast(StringType))

    val targetDocument = dfBody.filter($"paper_id" === paperId).select($"similarityDocument".as("similarityDocumentTarget"))


    val dfBodyTargeted = dfBody
      .crossJoin(targetDocument)
      .withColumn("similarityDocument",concat_ws(" ", $"similarityDocumentTarget", $"similarityDocument"))
      .select($"paper_id", $"similarityDocument")

    val model_name = "tfhub_use"

    val documentAssembler = new DocumentAssembler()
      .setInputCol("similarityDocument")
      .setOutputCol("document")

    val sentenceEncoder = UniversalSentenceEncoder
      .pretrained(model_name)
      .setInputCols("document")
      .setOutputCol("similarityScore")

    val pipeline = new Pipeline()
      .setStages(
        Array(
          documentAssembler,
          sentenceEncoder
        )
      )

    val pipelineModel = pipeline.fit(dfBodyTargeted)
    val pipelineDF = pipelineModel.transform(dfBodyTargeted)

    pipelineDF.write.mode(SaveMode.Overwrite).json("src/test/resources/har/test.json")

    val rankedSimilarity = pipelineDF
      .select(col("paper_id"), explode($"similarityScore.embeddings").as("mark"))
      .select(col("paper_id"), explode($"mark").as("mark"))
      .groupBy("paper_id")
      .agg(
        avg("mark").as("avgMark"),
        stddev("mark").as("stdDevMark"))
      .orderBy(desc("avgMark"))

    rankedSimilarity

  }

}
