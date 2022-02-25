package org.sunbird.dp.rating.task
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.core.job.BaseJobConfig
import org.sunbird.dp.rating.domain.Event

class RatingConfig (override val config: Config) extends BaseJobConfig(config, "RatingJob") {
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // User cache updater job metrics
  val userCacheHit = "user-cache-hit"
  val skipCount = "skipped-message-count"
  val successCount = "success-message-count"
  val dbReadSuccessCount = "db-read-success-count"
  val dbReadMissCount = "db-read-miss-count"
  val apiReadSuccessCount = "api-read-success-count"
  val apiReadMissCount = "api-read-miss-count"
  val totalEventsCount ="total-audit-events-count"


  // rating specific
  val ratingParallelism: Int = config.getInt("task.rating.parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val issueEventSink = "rating-issue-event-sink"
  val issueOutputTagName = "rating-issue-events"
  val issueOutputTag: OutputTag[Event] = OutputTag[Event]("rated-events")

  //Cassandra
  val courseTable: String = config.getString("lms-cassandra.course_table")
  val ratingsTable: String = config.getString("lms-cassandra.ratings_table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")

  // constants
  val userId = "userid"

  // Consumers
  val RatingConsumer = "rating-consumer"

  // Functions
  val ratingFunction = "RatingFunction"


}
