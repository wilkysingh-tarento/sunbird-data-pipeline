package org.sunbird.dp.rating.function

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.{CassandraUtil, JSONUtil}
import org.sunbird.dp.rating.domain.Event
import org.sunbird.dp.rating.task.RatingConfig

class RatingFunction(config: RatingConfig, @transient var cassandraUtil: CassandraUtil = null)(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[RatingFunction])

  private var dataCache: DataCache = _
  private var restUtil: RestUtil = _

  override def metricsList(): List[String] = {
    List()
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
//    restUtil = new RestUtil()
  }


  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {

    try {
      //job 1 starts
      val query = QueryBuilder.select().column("*").from(config.dbKeyspace, config.courseTable)
        .where(QueryBuilder.eq(config.userId, event.userId)).and(QueryBuilder.eq("active", "YES"))
      val rows: java.util.List[Row] = cassandraUtil.find(query.toString);
      if (null != rows && !rows.isEmpty) {
      println("user has enrolled to this course")
        //need to convert rows to map if needed
      } else {
        println("user is not enrolled to course and need to terminate the job with message USER NOT ENROLLED")
        Map[String, Int]()
      }
      //job 2 starts
      val ratingQuery = QueryBuilder.select().column("*").from(config.dbKeyspace, config.ratingsTable)
        .where(QueryBuilder.eq(config.userId, event.userId))
      val ratingRows: java.util.List[Row] = cassandraUtil.find(ratingQuery.toString);
      if (null != ratingRows && !ratingRows.isEmpty) {
        println("user already has a rating")
      } else {
        println("user is newly adding rating")
      }

      context.output(config.issueOutputTag,event)
   }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
        println(ex)
        logger.info("Event throwing exception: ", JSONUtil.serialize(event))
        throw ex
      }
    }
  }
}

