package org.ekstep.analytics.framework.filter

/**
 * @author Santhosh
 */
object EqualsMatcher extends IMatcher {
  
    def matchValue(value1: AnyRef, value2: Option[AnyRef]) : Boolean = {
        null != value1 && value1.equals(value2.getOrElse(null));
    }
}