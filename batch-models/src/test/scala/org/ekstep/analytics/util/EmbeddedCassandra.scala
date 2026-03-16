package org.ekstep.analytics.util

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.ekstep.analytics.framework.conf.AppConf

object EmbeddedCassandra {

  System.setProperty("cassandra.unsafesystem", "true");
  EmbeddedCassandraServerHelper.startEmbeddedCassandra(30000L);
  val connector = CassandraConnector(getSparkConf());
  val session = connector.openSession();
  val dataLoader = new CQLDataLoader(session);

  private def getSparkConf(): SparkConf = {
    val conf = new SparkConf().setAppName("TestAnalyticsCore");
    conf.setMaster("local[*]");
    conf.set("spark.cassandra.connection.port", AppConf.getConfig("cassandra.service.embedded.connection.port"))
    conf;
  }
  
  def setup() {
    dataLoader.load(new FileCQLDataSet(AppConf.getConfig("cassandra.cql_path"), true, true));
  }
  
  def loadData(cqlFile: String) {
    dataLoader.load(new FileCQLDataSet(cqlFile, false, false))
  }
  
  def close() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }
}