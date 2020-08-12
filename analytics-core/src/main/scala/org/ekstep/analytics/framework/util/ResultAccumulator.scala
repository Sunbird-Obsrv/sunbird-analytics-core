package org.ekstep.analytics.framework.util

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.ekstep.analytics.framework.conf.AppConf

import scala.collection.immutable

final class ResultAccumulator[E] extends GraphStage[FlowShape[E, immutable.Seq[E]]] {

    val in = Inlet[E]("ResultAccumulator.in")
    val out = Outlet[immutable.Seq[E]]("ResultAccumulator.out")

    override def shape = FlowShape.of(in, out)

    override def createLogic(attributes: Attributes) = new GraphStageLogic(shape) {

        private var counter: Int = 0
        private val buffer = Vector.newBuilder[E]

        setHandlers(in, out, new InHandler with OutHandler {

            override def onPush(): Unit = {
                val nextElement = grab(in)
                counter += 1

                if (counter < AppConf.getConfig("druid.query.batch.buffer").toLong) {
                    buffer += nextElement
                    pull(in)
                } else {
                    val result = buffer.result().toList
                    buffer.clear()
                    buffer += nextElement
                    counter = 0
                    push(out, result)
                }
            }

            override def onPull(): Unit = {
                pull(in)
            }

            override def onUpstreamFinish(): Unit = {
                val result = buffer.result().toList
                if (result.nonEmpty) {
                    emit(out, result)
                }
                completeStage()
            }
        })

        override def postStop(): Unit = {
            buffer.clear()
        }
    }
}