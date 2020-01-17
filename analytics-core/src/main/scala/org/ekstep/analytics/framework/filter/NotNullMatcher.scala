package org.ekstep.analytics.framework.filter

/**
 * @author Santhosh
 */
object NotNullMatcher extends IMatcher {
  
    def matchValue(value1: AnyRef, value2: Option[AnyRef]) : Boolean = {
        null != value1;
    }
}