package org.sunbird.dp.cbpreprocessor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
// import org.sunbird.dp.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.cbpreprocessor.domain.Event
import org.sunbird.dp.cbpreprocessor.task.CBPreprocessorConfig
// import org.sunbird.dp.cbpreprocessor.util.CBEventsFlattener

class CBPreprocessorFunction(config: CBPreprocessorConfig)(implicit val eventTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CBPreprocessorFunction])

  override def metricsList(): List[String] = {
    List(
      config.cbAuditEventMetricCount,
      config.cbWorkOrderRowMetricCount,
      config.cbAuditFailedMetricCount
    )
    /*List(
      config.cbAuditEventMetricCount,
      config.workOrderEventsMetricsCount,
      config.publishedWorkOrderEventsMetricsCount,
      config.workOrderDataRowMetricsCount,
      config.cbAuditEventRouterMetricCount
    ) ::: deduplicationMetrics*/
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // if (dedupEngine == null) {
    //  val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config)
    //  dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpirySeconds)
    // }
    // if (cbEventsFlattener == null) {
    //   cbEventsFlattener = CBEventsFlattener(config)
    // }
  }

  override def close(): Unit = {
    super.close()
    // dedupEngine.closeConnectionPool()
  }

//  def isDuplicateCheckRequired(producerId: String): Boolean = {
//    config.includedProducersForDedup.contains(producerId)
//  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {

    // node, competency/role/activity/workorder state (Draft, Approved, Published)
    context.output(config.cbAuditEventsOutputTag, event)
    metrics.incCounter(metric = config.cbAuditEventMetricCount)

    val hasWorkOrderData = event.hasWorkOrderData  // TODO: implement hasWorkOrderData() and isPublished()
    val isPublishedWorkOrder = hasWorkOrderData && event.isPublishedWorkOrder

    // increase counters
    // if (hasWorkOrderData) {
    //   metrics.incCounter(metric = config.workOrderEventsMetricsCount)
    //  if (isPublishedWorkOrder) {
    //    metrics.incCounter(metric = config.publishedWorkOrderEventsMetricsCount)
    //  }
    // }

    if (isPublishedWorkOrder) {
      // val workOrderRows = cbEventsFlattener.flatten(event, context, metrics)
      // workOrderRows.forEach(woRow => {
      //   context.output(config.cbWorkOrderRowOutputTag, woRow)
      //   metrics.incCounter(metric = config.cbWorkOrderRowMetricCount)
      // })
    }
  }

}
