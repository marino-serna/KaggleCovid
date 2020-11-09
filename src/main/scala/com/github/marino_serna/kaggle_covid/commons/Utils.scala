package com.github.marino_serna.kaggle_covid.commons

import org.apache.spark.sql.SparkSession

class Utils {
  val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
  import ss.implicits._
}
