package org.ekstep.analytics.framework.filter

/**
 * @author Santhosh
 */
object NotInMatcher extends IMatcher {

    def matchValue(value1: AnyRef, value2: Option[AnyRef]): Boolean = {
        if (value2.isEmpty || !(value2.get.isInstanceOf[List[AnyRef]])) {
            false;
        } else {
        	if(value1.isInstanceOf[List[AnyRef]]) {
        		!value2.get.asInstanceOf[List[AnyRef]].forall(value1.asInstanceOf[List[AnyRef]].contains);
        	} else {
        		!value2.get.asInstanceOf[List[AnyRef]].contains(value1);	
        	}
            
        }
    }
}