package org.ekstep.analytics.model

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.ekstep.analytics.framework.conf.AppConf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * @author Santhosh
 */
class BaseSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  def getSparkContext(): SparkContext = {
    getSparkSession().sparkContext;
  }

  def getSparkConf(): SparkConf = {
    val conf = new SparkConf().setAppName("AnalyticsTestSuite").set("spark.default.parallelism", "2");
    conf.set("spark.sql.shuffle.partitions", "2")
    conf.setMaster("local[*]")
    conf.set("spark.driver.memory", "1g")
    conf.set("spark.memory.fraction", "0.3")
    conf.set("spark.memory.storageFraction", "0.5")
    conf;
  }

  def getSparkSession() : SparkSession = {
    SparkSession.builder.config(getSparkConf()).getOrCreate()
  }

}
