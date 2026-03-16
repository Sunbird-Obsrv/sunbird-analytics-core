package org.ekstep.analytics.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.ekstep.analytics.framework.conf.AppConf
import org.scalatest._

class BaseSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

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
    conf.set("spark.cassandra.connection.port", AppConf.getConfig("cassandra.service.embedded.connection.port"))
    conf.set("es.nodes", "http://localhost")
    conf.set("spark.redis.host", "localhost")
    conf.set("spark.redis.port", "6341")
    conf.set("spark.redis.db", "0")
    conf;
  }

  def getSparkSession() : SparkSession = {
    SparkSession.builder.config(getSparkConf()).getOrCreate()
  }

}