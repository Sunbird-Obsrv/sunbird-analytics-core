package org.ekstep.analytics.framework.conf

import org.ekstep.analytics.framework.BaseSpec

/**
 * @author Santhosh
 */
class TestAppConf extends BaseSpec {
    
    "AppConf" should "initialize and get property" in {
        
        AppConf.getConfig("default.parallelization") should be ("10");
        AppConf.getConfig("xyz") should be ("");
        AppConf.getAwsKey() should be ("");
        AppConf.getAwsSecret() should be ("");
    }
  
}