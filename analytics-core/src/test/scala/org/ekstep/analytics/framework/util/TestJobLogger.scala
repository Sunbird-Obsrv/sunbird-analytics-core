package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.Level
import org.ekstep.analytics.framework.Level._

class TestJobLogger extends BaseSpec {

    implicit val className = "org.ekstep.analytics.framework.util.TestJobLogger"

    "JobLogger" should "pass test cases for all the methods in JobLogger" in {

        JobLogger.init(className);

        JobLogger.start("testing start method", Option("START"))
        JobLogger.log("testing info method", None, INFO)
        JobLogger.log("testing debug method", None, DEBUG)
        JobLogger.log("testing warn method", None, WARN)
        JobLogger.log("testing error method", None, ERROR)
        JobLogger.end("testing end method", "SUCCESS")
        JobLogger.end("testing end method", "FAILED")

    }

    it should "cover all cases" in {

        val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext];
        val config = ctx.getConfiguration();
        val loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.ALL);
        ctx.updateLoggers();

        JobLogger.log("testing info method", None, INFO);
        JobLogger.log("testing debug method", None, DEBUG);
        JobLogger.log("testing warn method", None, WARN);
        JobLogger.log("testing error method", None, ERROR);

        loggerConfig.setLevel(Level.OFF);
        ctx.updateLoggers();
        JobLogger.log("testing info method", None, INFO);
        JobLogger.log("testing debug method", None, DEBUG);
        JobLogger.log("testing warn method", None, WARN);
        JobLogger.log("testing error method", None, ERROR);

    }

}