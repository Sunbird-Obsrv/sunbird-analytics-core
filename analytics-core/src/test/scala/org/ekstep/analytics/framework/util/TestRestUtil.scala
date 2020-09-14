package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec
import org.ekstep.analytics.framework.Metadata
import org.ekstep.analytics.framework.Request
import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.Search
import org.ekstep.analytics.framework.SearchFilter

import com.fasterxml.jackson.core.JsonParseException
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.Params
import com.google.common.net.InetAddresses

/**
 * @author Santhosh
 */
case class PostR(args: Map[String, String], data: String, headers: Map[String, String], json: Map[String, AnyRef], origin: String, url: String);
case class PostErrR(args: Map[String, String], data: String, headers: Map[String, String], json: String, origin: String, url: String);
case class GetR(origin: String);

class TestRestUtil extends BaseSpec {

    // TODO:  Need to fix the Test cases with proper request
    /* "RestUtil" should "execute GET and parse response" in { 
        val url = "https://httpbin.org/ip";
        val response = RestUtil.get[GetR](url);
        response should not be null; 
        response.origin should not be null;
        InetAddresses.isInetAddress(response.origin) should be(true);
    } */

    it should "throw Exception if unable to parse the response during GET" in {
        val url = "https://httpbin.org/xml";
        val response = RestUtil.get[GetR](url);
        response should be(null);
        
        val url2 = "https://httpbin.org/xml";
        val response2 = RestUtil.get[String](url2, Option(Map("Content-Type" -> "application/json")));
        response2 should not be(null);
    }

    // TODO:  Need to fix the Test cases with proper request
    it should "execute POST and parse response" in {
        val url = "https://httpbin.org/post?type=test";
        val response = RestUtil.post[PostR](url, "", Option(Map("accept" -> "application/json")));
        response should not be null;
        response.url should be("https://httpbin.org/post?type=test");
        InetAddresses.isInetAddress(response.origin) should be(true);
    }

    it should "throw Exception if unable to parse the response during POST" in {
        val url = "https://httpbin.org/post?type=test";
        val request = Map("popularity" -> 1);
        val response = RestUtil.post[PostErrR](url, JSONUtils.serialize(request));
        response should be(null);
    }

    // TODO:  Need to fix the Test cases with proper request
    it should "execute PATCH and parse response" in {
        val url = "https://httpbin.org/patch?type=test";
        val request = Map("popularity" -> 1);
        val response = RestUtil.patch[PostR](url, JSONUtils.serialize(request), Option(Map("accept" -> "application/json")));
        response should not be null;
        response.url should be("https://httpbin.org/patch?type=test");
        InetAddresses.isInetAddress(response.origin) should be(true);
        response.data should be("{\"popularity\":1}");
        response.json.get("popularity").get should be(1);
    } 
    
    it should "execute PUT and parse response" in {
        val url = "https://httpbin.org/put?type=test";
        val request = Map("popularity" -> 1);
        val response = RestUtil.put[PostR](url, JSONUtils.serialize(request), Option(Map("accept" -> "application/json")));
        response should not be null;
        response.url should be("https://httpbin.org/put?type=test");
        InetAddresses.isInetAddress(response.origin) should be(true);
        response.data should be("{\"popularity\":1}");
        response.json.get("popularity").get should be(1);
        
        val url2 = "https://httpbin.org/put?type=test";
        val response2 = RestUtil.put[PostErrR](url2, JSONUtils.serialize(request));
        response2 should be(null);
    }
    
    it should "execute Delete and parse response" in {
        val url = "https://httpbin.org/delete";
        val response = RestUtil.delete[PostR](url, Option(Map("accept" -> "application/json")));
        response should not be null;
        response.url should be("https://httpbin.org/delete");
        InetAddresses.isInetAddress(response.origin) should be(true);
        
        val url2 = "https://httpbin.org/delete/xml";
        val response2 = RestUtil.delete[PostErrR](url2, None);
        response2 should be (null);
    }

    it should "throw Exception if unable to parse the response during PATCH" in {
        val url = "https://httpbin.org/patch?type=test";
        val request = Map("popularity" -> 1);
        val response = RestUtil.patch[PostErrR](url, JSONUtils.serialize(request));
        response should be(null);
    }

}