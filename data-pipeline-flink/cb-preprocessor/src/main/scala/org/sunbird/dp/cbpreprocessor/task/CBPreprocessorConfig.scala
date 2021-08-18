package org.sunbird.dp.cbpreprocessor.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.core.job.BaseJobConfig
import org.sunbird.dp.cbpreprocessor.domain.Event

import scala.collection.JavaConverters._

class CBPreprocessorConfig(override val config: Config) extends BaseJobConfig(config, "CBPreprocessorJob") {

  private val serialVersionUID = 2905979434303791379L  // TODO: change this?

  implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val schemaPath: String = config.getString("telemetry.schema.path")

  val dedupStore: Int = config.getInt("redis.database.duplicationstore.id")
  val cacheExpirySeconds: Int = config.getInt("redis.database.key.expiry.seconds")

  // Kafka Topic Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")

  val kafkaCbAuditOutputRouteTopic: String = config.getString("kafka.output.cb.audit.druid.route.topic")

  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")
  val kafkaDuplicateTopic: String = config.getString("kafka.output.duplicate.topic")

  val defaultChannel: String = config.getString("default.channel")

  val includedProducersForDedup: List[String] = config.getStringList("dedup.producer.included.ids").asScala.toList

  //
  val auditRouteEventsOutputTag: OutputTag[Event] = OutputTag[Event]("audit-route-events")
  val cbAuditRouteEventsOutputTag: OutputTag[Event] = OutputTag[Event]("cb-audit-route-events")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val downstreamOperatorsParallelism: Int = config.getInt("task.downstream.operators.parallelism")

  // Router job metrics
  val primaryRouterMetricCount = "primary-route-success-count"
  val cbAuditEventRouterMetricCount = "cb-audit-route-success-count"

  // Validation job metrics
  val validationSuccessMetricsCount = "validation-success-event-count"
  val validationFailureMetricsCount = "validation-failed-event-count"
  val duplicationEventMetricsCount = "duplicate-event-count"
  val duplicationSkippedEventMetricsCount = "duplicate-skipped-event-count"
  val uniqueEventsMetricsCount = "unique-event-count"
  val validationSkipMetricsCount = "validation-skipped-event-count"

  // Consumers
  val cbPreprocessorConsumer = "cb-preprocessor-consumer"

  // Producers
  val primaryRouterProducer = "primary-route-sink"
  val cbAuditRouterProducer = "cb-audit-route-sink"
  val invalidEventProducer = "invalid-events-sink"
  val duplicateEventProducer = "duplicate-events-sink"

  val defaultSchemaFile = "envelope.json"

}
