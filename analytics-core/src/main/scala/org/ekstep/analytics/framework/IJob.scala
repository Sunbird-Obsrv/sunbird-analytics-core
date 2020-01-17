package org.ekstep.analytics.framework

import org.apache.spark.SparkContext

trait IJob {
    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None);
}