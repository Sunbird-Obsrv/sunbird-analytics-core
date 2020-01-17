package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.BaseSpec
import org.ekstep.analytics.framework.FrameworkContext

/**
 * @author Santhosh
 */
class TestConsoleDispatcher extends BaseSpec {
    
    implicit val fc = new FrameworkContext();
    
    "ConsoleDispatcher" should "send output to console" in {
        
        val events = ConsoleDispatcher.dispatch(Array[String]("test"), Map[String, AnyRef]());
        events.length should be (1);
        events(0) should be ("test");
        
        val out = ConsoleDispatcher.dispatch(Array[String]("test"), Map("printEvent" -> false.asInstanceOf[AnyRef]));
        out.length should be (1);
    }
  
}