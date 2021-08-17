package org.sunbird.dp.cbpreprocessor.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.util.FlinkUtil
import org.sunbird.dp.cbpreprocessor.domain.Event
import org.sunbird.dp.cbpreprocessor.functions.CBPreprocessorFunction

import java.io.File

/**
 *
 */

class CBPreprocessorStreamTask(config: CBPreprocessorConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = 146697324640926024L

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    val kafkaConsumer = kafkaConnector.kafkaEventSource[Event](config.kafkaInputTopic)

    val eventStream: SingleOutputStreamOperator[Event] =
      env.addSource(kafkaConsumer, config.cbPreprocessorConsumer)
        .uid(config.cbPreprocessorConsumer).setParallelism(config.kafkaConsumerParallelism)
        .rebalance()
        .process(new CBPreprocessorFunction(config)).setParallelism(config.downstreamOperatorsParallelism)

    // TODO: REPLACE
    /**
     * Routing FRAC and WAT events to frac and wat
     */
    eventStream.getSideOutput(config.logEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaLogRouteTopic))
      .name(config.logRouterProducer).uid(config.logRouterProducer)
      .setParallelism(config.downstreamOperatorsParallelism)

    eventStream.getSideOutput(config.errorEventOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaErrorRouteTopic))
      .name(config.errorRouterProducer).uid(config.errorRouterProducer)
      .setParallelism(config.downstreamOperatorsParallelism)

    /**
     * Pushing "AUDIT" event into both sink and audit topic
     */
    eventStream.getSideOutput(config.auditRouteEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaAuditRouteTopic))
      .name(config.auditRouterProducer).uid(config.auditRouterProducer)
      .setParallelism(config.downstreamOperatorsParallelism)

    eventStream.getSideOutput(config.auditRouteEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaPrimaryRouteTopic))
      .name(config.auditEventsPrimaryRouteProducer).uid(config.auditEventsPrimaryRouteProducer)
      .setParallelism(config.downstreamOperatorsParallelism)

    /**
     * pushing cbAudit events into cbAudit topic
     */

    eventStream.getSideOutput(config.cbAuditRouteEventsOutputTag)
      .addSink(kafkaConnector.kafkaEventSink[Event](config.kafkaCbAuditRouteTopic))
      .name(config.cbAuditRouterProducer).uid(config.cbAuditRouterProducer)
      .setParallelism(config.downstreamOperatorsParallelism)

    // TODO: REPLACE, END

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object CBPreprocessorStreamTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("cb-preprocessor.conf").withFallback(ConfigFactory.systemEnvironment()))
    val cbPreprocessorConfig = new CBPreprocessorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(cbPreprocessorConfig)
    val task = new CBPreprocessorStreamTask(cbPreprocessorConfig, kafkaUtil)
    task.process()
  }
}
// $COVERAGE-ON$
