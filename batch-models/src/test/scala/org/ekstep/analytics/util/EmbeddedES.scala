package org.ekstep.analytics.util

import java.util.concurrent.TimeUnit.MINUTES

import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, IndexRequest, IndexSettings}
import pl.allegro.tech.embeddedelasticsearch.PopularProperties.{CLUSTER_NAME, HTTP_PORT}

import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

case class EsIndex(index: String, indexType: Option[String], mappingSettings: Option[String], aliasSettings: Option[String])

object EmbeddedES {

  var esServer: EmbeddedElastic = null;

  def start(indices: Array[EsIndex]) {
    val builder = EmbeddedElastic.builder()
      .withElasticVersion("6.3.0")
      .withSetting(HTTP_PORT, "9200")
      .withSetting(CLUSTER_NAME, "TestCluster")
      .withEsJavaOpts("-Xms128m -Xmx1g")
      .withStartTimeout(2, MINUTES);

    indices.foreach(f => {
      val indexSettingsBuilder = IndexSettings.builder();
      if (f.mappingSettings.nonEmpty) indexSettingsBuilder.withType(f.indexType.get, f.mappingSettings.get)
      if (f.aliasSettings.nonEmpty) indexSettingsBuilder.withAliases(f.aliasSettings.get)
      builder.withIndex(f.index, indexSettingsBuilder.build())
    })
    esServer = builder.build().start();
  }

  def loadData(indexName: String, indexType: String, indexData: Buffer[String]) = {
    val docs = indexData.map(f => {
      new IndexRequest.IndexRequestBuilder(indexName, indexType, f).build()
    })
    esServer.index(JavaConverters.bufferAsJavaListConverter(docs).asJava);
  }

  def index(indexName: String, indexType: String, data: String, id: String) = {
    val doc = new IndexRequest.IndexRequestBuilder(indexName, indexType, data).withId(id).build()
    esServer.index(JavaConverters.bufferAsJavaListConverter(Buffer(doc)).asJava);
  }

  def getAllDocuments(index: String): Buffer[String] = {
    esServer.fetchAllDocuments(index).asScala;
  }

  def stop() {
    if (esServer != null) {
      esServer.stop();
      Console.println("****** Stopping the embedded elastic search service ******");
    } else {
      Console.println("****** Already embedded ES is stopped ******");
    }

  }
}