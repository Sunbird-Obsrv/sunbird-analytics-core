package org.ekstep.analytics.framework.filter

/**
 * @author Santhosh
 */
trait IMatcher {
  
    def matchValue(value1: AnyRef, value2: Option[AnyRef]) : Boolean;
}