package com.github.marino_serna.kaggle_covid.commons

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait SparkTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {
  var ss: SparkSession = _
  var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val master = "local[*]"
    val appName = "kaggle_covid"
    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.driver.allowMultipleContexts", "false")
      .set("spark.ui.enabled", "false")

    ss = SparkSession.builder().config(conf).getOrCreate()
    sc = ss.sparkContext

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }
}