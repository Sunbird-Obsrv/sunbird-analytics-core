package org.ekstep.analytics.framework.filter

import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.exception.DataFilterException
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._

/**
 * @author Santhosh
 */
object Matcher {

    @throws(classOf[DataFilterException])
    def getMatcher(op: String): IMatcher = {
        op match {
            case "NE"         => NotEqualsMatcher;
            case "IN"         => InMatcher;
            case "NIN"        => NotInMatcher;
            case "ISNULL"     => NullMatcher;
            case "ISEMPTY"    => EmptyMatcher;
            case "ISNOTNULL"  => NotNullMatcher;
            case "ISNOTEMPTY" => NotEmptyMatcher;
            case "EQ"         => EqualsMatcher;
            case "RANGE"      => RangeMatcher;
            case  "LT"        => LessThanMatcher;
            case _ =>
                throw new DataFilterException("Unknown filter operation found");
        }
    }

    @throws(classOf[DataFilterException])
    def compare(value: AnyRef, filter: Filter): Boolean = {
        getMatcher(filter.operator).matchValue(value, filter.value);
    }
}