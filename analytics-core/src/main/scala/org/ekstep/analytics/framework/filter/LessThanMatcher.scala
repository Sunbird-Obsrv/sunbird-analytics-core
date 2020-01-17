package org.ekstep.analytics.framework.filter

import java.text.SimpleDateFormat
import java.util.Date


object LessThanMatcher extends IMatcher {

  def matchValue(value1: AnyRef, value2: Option[AnyRef]): Boolean = {

    if (null == value2.getOrElse(null)) {
      return false;
    }


    if (value1.isInstanceOf[Long]) {
      if (value1.asInstanceOf[Long] < value2.get.asInstanceOf[Long]) {
        return true;
      }
    }

    if (value1.isInstanceOf[Double]) {
      if (value1.asInstanceOf[Double] < value2.get.asInstanceOf[Double]) {
        return true;
      }
    }

    if (value1.isInstanceOf[Int]) {
      if (value1.asInstanceOf[Int] < value2.get.asInstanceOf[Int]) {
        return true;
      }
    }

    if (value1.isInstanceOf[Date]) {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      if (value1.asInstanceOf[Date].before(sdf.parse(value2.get.asInstanceOf[String]))) {
        return true;
      }
    }

    return false;
  }

}