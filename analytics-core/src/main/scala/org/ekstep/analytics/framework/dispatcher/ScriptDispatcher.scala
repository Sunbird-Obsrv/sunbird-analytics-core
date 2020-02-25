package org.ekstep.analytics.framework.dispatcher

import scala.io.Source
import java.io.PrintWriter
import org.ekstep.analytics.framework.exception.DispatcherException
import sys.process._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.FrameworkContext

/**
 * @author Santhosh
 */
object ScriptDispatcher extends IDispatcher {

    val className = "org.ekstep.analytics.framework.dispatcher.ScriptDispatcher"

    @deprecated
    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef])(implicit fc: FrameworkContext): Array[String] = {
        val script = config.getOrElse("script", null).asInstanceOf[String];
        if (null == script) {
            throw new DispatcherException("'script' parameter is required to send output to file")
        }
        val envParams = config.map(f => f._1 + "=" + f._2.asInstanceOf[String]).toArray;
        val proc = Runtime.getRuntime.exec(script, envParams);
        new Thread("stderr reader for " + script) {
            override def run() {
                for (line <- Source.fromInputStream(proc.getErrorStream).getLines)
                    Console.err.println(line)
            }
        }.start();
        new Thread("stdin writer for " + script) {
            override def run() {
                val out = new PrintWriter(proc.getOutputStream)
                for (elem <- events)
                    out.println(elem)
                out.close()
            }
        }.start();
        val outputLines = Source.fromInputStream(proc.getInputStream).getLines;
        val exitStatus = proc.waitFor();
        if (exitStatus != 0) {
            throw new DispatcherException("Script exited with non zero status")
        }
        outputLines.toArray;
    }
    
    def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext, fc: FrameworkContext) = {
        dispatch(events.collect(), config);
    }

    def dispatch(script: String): Int = {
        if (null == script) {
            throw new DispatcherException("script should not be empty")
        }
        script.!
    }

    def dispatch(script: ProcessBuilder): Int = {
        if (null == script) {
            throw new DispatcherException("script cannot be null")
        }
        script.!
    }

}