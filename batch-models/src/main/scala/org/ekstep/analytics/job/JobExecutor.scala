package org.ekstep.analytics.job

import org.ekstep.analytics.framework.FrameworkContext

/**
 * @author Santhosh
 */
object JobExecutor {

    val className = "org.ekstep.analytics.job.JobExecutor"

    def main(args: Array[String]): Unit = {
        val params = parseArgs(args)
        val model = params.getOrElse("model", throw new IllegalArgumentException("--model is required"))
        val config = params.getOrElse("config", throw new IllegalArgumentException("--config is required"))
        println("inside Job Executor main method")
        implicit val fc: FrameworkContext = null
        val job = JobFactory.getJob(model)
        job.main(config)(None, Option(fc))
    }

    private def parseArgs(args: Array[String]): Map[String, String] = {
        args.sliding(2, 2).collect {
            case Array(key, value) if key.startsWith("--") => key.stripPrefix("--") -> value
        }.toMap
    }
}

object JobExecutorV2 extends optional.Application {

    val className = "org.ekstep.analytics.job.JobExecutorV2"

    def main(model: String, config: String)(implicit fc : FrameworkContext) {
        val job = JobFactory.getJob(model);
        job.main(config)(None, Option(fc))
    }
}