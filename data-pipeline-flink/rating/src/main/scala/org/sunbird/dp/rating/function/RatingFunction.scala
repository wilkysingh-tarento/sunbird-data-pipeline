package org.sunbird.dp.rating.function

import com.google.gson.{Gson, JsonObject}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.JSONUtil
import org.sunbird.dp.rating.domain.Event
import org.sunbird.dp.rating.task.RatingConfig

import java.util
import scala.collection.JavaConverters.mapAsJavaMap
import scala.collection.mutable

class RatingFunction(config: RatingConfig)(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[RatingFunction])
  private var dataCache: DataCache = _

  private var restUtil: RestUtil = _

  override def metricsList(): List[String] = {
    List(config.userCacheHit, config.skipCount, config.successCount, config.totalEventsCount, config.apiReadMissCount, config.apiReadSuccessCount)
  }

//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//    dataCache = new DataCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config), config.userStore, config.userFields)
//    dataCache.init()
//    restUtil = new RestUtil()
//  }

//  override def close(): Unit = {
//    super.close()
//    dataCache.close()
//  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {

    try {
      println("1")
      println(event)
      context.output(config.issueOutputTag,event)


    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        println(ex)
        logger.info("Event throwing exception: ", JSONUtil.serialize(event))
        throw ex
      }
    }
  }
}

