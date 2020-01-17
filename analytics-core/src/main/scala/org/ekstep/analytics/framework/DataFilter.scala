package org.ekstep.analytics.framework

import org.apache.commons.beanutils.PropertyUtils
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.exception.DataFilterException
import org.ekstep.analytics.framework.filter.Matcher
import org.ekstep.analytics.framework.util.{CommonUtil, JobLogger}

import scala.collection.mutable.Buffer
import scala.util.control.Breaks

/**
 * @author Santhosh
 */
object DataFilter {

    implicit val className = "org.ekstep.analytics.framework.DataFilter"

    /**
     * Execute multiple filters and sort
     */
    @throws(classOf[DataFilterException])
    def filterAndSort[T](events: RDD[T], filters: Option[Array[Filter]], sort: Option[Sort]): RDD[T] = {
        JobLogger.log("Running the filter and sort process", Option(Map("filter" -> filters, "sort" -> sort)));
        val filteredEvents = if (filters.nonEmpty) { filter(events, filters.get) } else events;
        if (sort.nonEmpty) { sortBy(filteredEvents, sort.get) } else filteredEvents;
    }

    @throws(classOf[DataFilterException])
    def filter[T](events: RDD[T], filters: Array[Filter]): RDD[T] = {
        JobLogger.log("Running the filter process");
        if (null != filters && filters.nonEmpty) {
            events.filter { event =>
                var valid = true;
                Breaks.breakable {
                    filters.foreach { filter =>
                        val value = getValue(event, filter.name);
                        valid = Matcher.compare(value, filter);
                        if (!valid) Breaks.break;
                    }
                }
                valid;
            }
        } else {
            events;
        }
    }

    @throws(classOf[DataFilterException])
    def filter[T](events: RDD[T], filter: Filter): RDD[T] = {
        if (null != filter) {
            events.filter { event =>
                val value = getValue(event, filter.name);
                Matcher.compare(value, filter);
            }
        } else {
            events;
        }
    }

    @throws(classOf[DataFilterException])
    def matches[T](event: T, filter: Filter): Boolean = {
        if (null != filter) {
            val value = getValue(event, filter.name);
            Matcher.compare(value, filter);
        } else {
            true
        }
    }
    
    
    @throws(classOf[DataFilterException])
    def filter[T, S](events: RDD[T], value: S, filter: (T, S) => Boolean): RDD[T] = {
        if (null != filter) {
            events.filter { event => filter(event, value) };
        } else {
            events;
        }
    }

    @throws(classOf[DataFilterException])
    def matches[T](event: T, filters: Array[Filter]): Boolean = {
        if (null != filters) {
            var valid = true;
            Breaks.breakable {
                filters.foreach { filter =>
                    val value = getValue(event, filter.name);
                    valid = Matcher.compare(value, filter);
                    if (!valid) Breaks.break;
                }
            }
            valid;
        } else {
            true
        }
    }

    @throws(classOf[DataFilterException])
    def filter[T](events: Buffer[T], filter: Filter): Buffer[T] = {
        if (null != filter) {
            events.filter { event =>
                val value = getValue(event, filter.name);
                Matcher.compare(value, filter);
            }
        } else {
            events;
        }
    }

    def sortBy[T](events: RDD[T], sort: Sort): RDD[T] = {
        if (null != sort) {
            events.sortBy(f => getStringValue(f, sort.name), "asc".equalsIgnoreCase(sort.order.getOrElse("asc")), JobContext.parallelization);
        } else {
            events;
        }
    }

    private def getStringValue(event: Any, name: String): String = {
        val value = getValue(event, name);
        if (null == value) "" else value.toString()
    }

    private def getValue(event: Any, name: String): AnyRef = {
        name match {
            case "eventId" => getBeanProperty(event, "eid");
            case "ts"      => CommonUtil.getTimestamp(getBeanProperty(event, "ts").asInstanceOf[String]).asInstanceOf[AnyRef];
            case "eventts" =>
                if (event.isInstanceOf[Event]) {
                    CommonUtil.getTimestamp(getBeanProperty(event, "metadata.sync_timestamp").asInstanceOf[String]).asInstanceOf[AnyRef];
                } else if (event.isInstanceOf[MeasuredEvent]) {
                    getBeanProperty(event, "syncts").asInstanceOf[AnyRef];
                } else {
                    val eventMap = CommonUtil.caseClassToMap(event)
                    CommonUtil.getTimestamp(eventMap.get("$attimestamp").get.asInstanceOf[String]).asInstanceOf[AnyRef];
                }
            case "gameId" =>
                val gid = getBeanProperty(event, "edata.eks.gid");
                if (null == gid)
                    getBeanProperty(event, "gdata.id");
                else
                    gid;
            case "genieTag" =>
                val tags = if(event.isInstanceOf[Event]) CommonUtil.getETags(event.asInstanceOf[Event]).app else getBeanProperty(event, "etags").asInstanceOf[ETags].app;
                if (tags.isDefined) tags.get else List()
            case "gameVersion"      => getBeanProperty(event, "gdata.ver");
            case "userId"           => getBeanProperty(event, "uid");
            case "sessionId"        => getBeanProperty(event, "sid");
            case "telemetryVersion" => getBeanProperty(event, "ver");
            case "itemId"           => getBeanProperty(event, "edata.eks.qid");
            case _                  => getBeanProperty(event, name);
        }
    }

    private def getBeanProperty(event: Any, prop: String): AnyRef = {
        val obj = PropertyUtils.getProperty(event, prop);
        if (null != obj) {
            val objClass = obj.getClass.getName;
            objClass match {
                case "scala.Some" =>
                    obj.asInstanceOf[Some[AnyRef]].get;
                case "scala.None$" => null;
                case _             => obj.asInstanceOf[AnyRef];
            }
        } else {
            obj;
        }
    }

}