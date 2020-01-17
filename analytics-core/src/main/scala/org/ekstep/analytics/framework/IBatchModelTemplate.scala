package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

trait Input extends AnyRef with Serializable
trait AlgoInput extends Any with Serializable
trait AlgoOutput extends AnyRef with Serializable
trait Output extends AnyRef with Serializable

trait IBatchModelTemplate[T <: AnyRef, A <: AlgoInput, B <: AlgoOutput, R <: Output] extends IBatchModel[T, R] {

    /**
     * Override and implement the data product execute method. In addition to controlling the execution this base class records all generated RDD's,
     * so that they can be cleaned up from memory when necessary. This invokes all the three stages defined for a data product in the following order
     * 1. preProcess
     * 2. algorithm
     * 3. postProcess
     */
    override def execute(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext, fc: FrameworkContext): RDD[R] = {
        
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val inputRDD = preProcess(events, config);
        JobContext.recordRDD(inputRDD);
        val outputRDD = algorithm(inputRDD, config);
        JobContext.recordRDD(outputRDD);
        val resultRDD = postProcess(outputRDD, config);
        JobContext.recordRDD(resultRDD);
        resultRDD
    }

    /**
     * Pre processing steps before running the algorithm. Few pre-process steps are
     * 1. Transforming input - Filter/Map etc.
     * 2. Join/fetch data from LP
     * 3. Join/Fetch data from Cassandra
     */
    def preProcess(events: RDD[T], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[A]

    /**
     * Method which runs the actual algorithm
     */
    def algorithm(events: RDD[A], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[B]

    /**
     * Post processing on the algorithm output. Some of the post processing steps are
     * 1. Saving data to Cassandra
     * 2. Converting to "MeasuredEvent" to be able to dispatch to Kafka or any output dispatcher
     * 3. Transform into a structure that can be input to another data product
     */
    def postProcess(events: RDD[B], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[R]
}