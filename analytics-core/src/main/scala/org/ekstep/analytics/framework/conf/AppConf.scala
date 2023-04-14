package org.ekstep.analytics.framework.conf

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

object AppConf {

    implicit val className = "org.ekstep.analytics.framework.conf.AppConf";

    lazy val defaultConf = ConfigFactory.load();
    lazy val envConf = ConfigFactory.systemEnvironment();
    lazy val conf = defaultConf.withFallback(envConf);

    def getConfig(key: String): String = {
        val keyValue = ""
        if (conf.hasPath(key))
            keyValue = conf.getString(key);
        // else 
        //     keyValue = "";
        println(s"The value for $key is $keyValue")
        return keyValue
    }
    
    def getConfig(): Config = {
    	return conf;
    }

    def getAwsKey(): String = {
        getConfig("aws_key");
    }

    def getAwsSecret(): String = {
        getConfig("aws_secret");
    }

}