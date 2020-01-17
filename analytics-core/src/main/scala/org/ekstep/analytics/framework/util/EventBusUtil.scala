package org.ekstep.analytics.framework.util

import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe

object EventBusUtil {
  
    lazy val eventBus = new EventBus();
    
    def dipatchEvent(event: String) {
        eventBus.post(event);
    }
    
    def register(listener: Any) {
        eventBus.register(listener);
    }
    
}