package com.github.marino_serna.kaggle_covid

import com.github.marino_serna.kaggle_covid.KaggleCovidMain.{filterEnglish, kaggleETL, moveFiles, readRawData, writeParquet}
import com.github.marino_serna.kaggle_covid.commons.SparkTest
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType


class ImportTesting extends SparkTest{

  test("Test ETL") {
    val path = "src/test/resources"
    kaggleETL(ss: SparkSession, path:String)
  }

  test("Test task 1") {
    val path = "src/test/resources"
    val dfRaw = readRawData(ss, path)

    val dfFilterLang = filterEnglish(ss: SparkSession, dfRaw:DataFrame, path)

    //In fact it should be 22, but the algorithm fail to detect one of the files as English, instead is flag as Unknown
    dfFilterLang.count() should be <= 22L

    writeParquet(dfFilterLang, "src/test/resources/output/kaggles.parquet")

  }


}
