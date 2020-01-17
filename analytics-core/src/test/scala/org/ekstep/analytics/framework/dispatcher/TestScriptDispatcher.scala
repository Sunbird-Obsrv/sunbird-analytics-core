package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.BaseSpec
import org.ekstep.analytics.framework.FrameworkContext

class TestScriptDispatcher extends BaseSpec {
    
    implicit val fc = new FrameworkContext();
    
    "ScriptDispatcher" should "execute the script" in {
        
        val events = ScriptDispatcher.dispatch("ls");
        
        a[Exception] should be thrownBy {
            ScriptDispatcher.dispatch(null);
        }
    }
  
}