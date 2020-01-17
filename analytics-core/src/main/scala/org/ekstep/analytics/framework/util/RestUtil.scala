package org.ekstep.analytics.framework.util

import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.ekstep.analytics.framework.Level._

import scala.io.Source

trait HTTPClient {
    def get[T](apiURL: String)(implicit mf: Manifest[T]): T
    def post[T](apiURL: String, body: String, requestHeaders: Option[Map[String, String]] = None)(implicit mf: Manifest[T]): T
    def patch[T](apiURL: String, body: String, headers: Option[Map[String,String]] = None)(implicit mf: Manifest[T]): T
    def put[T](apiURL:String, body:String,headers:Option[Map[String,String]] = None)(implicit mf:Manifest[T]):T
    def delete[T](apiURL: String, requestHeaders: Option[Map[String, String]])(implicit mf: Manifest[T]):T
}

/**
 * @author Santhosh
 */
object RestUtil extends HTTPClient{

    implicit val className = "org.ekstep.analytics.framework.util.RestUtil"

    private def _call[T](request: HttpRequestBase)(implicit mf: Manifest[T]) = {

        val httpClient = HttpClients.createDefault();
        try {
            val httpResponse = httpClient.execute(request);
            val entity = httpResponse.getEntity
            val inputStream = entity.getContent
            val content = Source.fromInputStream(inputStream, "UTF-8").getLines.mkString;
            inputStream.close
            if ("java.lang.String".equals(mf.toString())) {
                content.asInstanceOf[T];
            } else {
                JSONUtils.deserialize[T](content);
            }
        }finally {
            httpClient.close()
        }
    }

    def get[T](apiURL: String)(implicit mf: Manifest[T]) = {
        val request = new HttpGet(apiURL);
        request.addHeader("user-id", "analytics");
        try {
            _call(request.asInstanceOf[HttpRequestBase]);
        } catch {
            case ex: Exception =>
                JobLogger.log(ex.getMessage, Option(Map("url" -> apiURL)), ERROR)
                ex.printStackTrace();
                null.asInstanceOf[T];
        }
    }

    def post[T](apiURL: String, body: String, requestHeaders: Option[Map[String, String]] = None)(implicit mf: Manifest[T]) = {

        val request = new HttpPost(apiURL)
        request.addHeader("user-id", "analytics")
        request.addHeader("Content-Type", "application/json")
        requestHeaders.getOrElse(Map()).foreach {
            case (headerName, headerValue) => request.addHeader(headerName, headerValue)
        }
        request.setEntity(new StringEntity(body))
        try {
            _call(request.asInstanceOf[HttpRequestBase])
        } catch {
            case ex: Exception =>
                JobLogger.log(ex.getMessage, Option(Map("url" -> apiURL, "body" -> body)), ERROR)
                ex.printStackTrace()
                null.asInstanceOf[T]
        }
    }

    def patch[T](apiURL: String, body: String, headers: Option[Map[String,String]] = None)(implicit mf: Manifest[T]) = {

        val request = new HttpPatch(apiURL);
        request.addHeader("user-id", "analytics");
        request.addHeader("Content-Type", "application/json");
        headers.getOrElse(Map()).foreach { header =>
            request.addHeader(header._1, header._2)
        }
        request.setEntity(new StringEntity(body));
        try {
            _call(request.asInstanceOf[HttpRequestBase]);
        } catch {
            case ex: Exception =>
                JobLogger.log(ex.getMessage, Option(Map("url" -> apiURL, "body" -> body)), ERROR)
                ex.printStackTrace();
                null.asInstanceOf[T];
        }
    }
    def put[T](apiURL: String, body: String, headers: Option[Map[String,String]] = None)(implicit mf: Manifest[T]) = {

        val request = new HttpPut(apiURL)
        request.addHeader("user-id", "analytics")
        request.addHeader("Content-Type", "application/json");
        headers.getOrElse(Map()).foreach { header =>
            request.addHeader(header._1, header._2)
        }
        request.setEntity(new StringEntity(body));
        try {
            _call(request.asInstanceOf[HttpRequestBase]);
        } catch {
            case ex: Exception =>
                JobLogger.log(ex.getMessage, Option(Map("url" -> apiURL, "body" -> body)), ERROR)
                ex.printStackTrace();
                null.asInstanceOf[T];
        }
    }
    def delete[T](apiURL: String, headers: Option[Map[String,String]] = None)(implicit mf: Manifest[T]) = {
        val request = new HttpDelete(apiURL)
        headers.getOrElse(Map()).foreach { header =>
            request.addHeader(header._1, header._2)
        }
        request.addHeader("user-id", "analytics")
        request.addHeader("Content-Type", "application/json");
        try {
            _call(request.asInstanceOf[HttpRequestBase]);
        } catch {
            case ex: Exception =>
                JobLogger.log(ex.getMessage, Option(Map("url" -> apiURL)), ERROR)
                ex.printStackTrace();
                null.asInstanceOf[T];
        }
    }
}