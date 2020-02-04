package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import java.io.FileWriter
import org.ekstep.analytics.framework.OutputDispatcher
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.commons.lang3.StringUtils
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.FrameworkContext
import org.apache.hadoop.conf.Configuration

/**
 * @author Santhosh
 */
trait HadoopDispatcher {

    def dispatchData(srcFile: String, destFile: String, conf: Configuration, events: RDD[String])(implicit fc: FrameworkContext) = {

        val fileUtil = fc.getHadoopFileUtil();
        fileUtil.delete(conf, srcFile, destFile);
        events.saveAsTextFile(srcFile);
        fileUtil.copyMerge(srcFile, destFile, conf, true);
    }

}