package org.sunbird.dp.rating.task
import com.google.gson.JsonObject
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.core.job.BaseJobConfig
import org.sunbird.dp.rating.domain.Event

import java.util
import java.util.{List => JList}

class RatingConfig (override val config: Config) extends BaseJobConfig(config, "RatingJob") {
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")
  val userFields: List[String] = if (config.hasPath("user.redis.removeable-fields"))
    config.getStringList("user.redis.removeable-fields").asInstanceOf[List[String]]
  else List[String]("state", "district", "block", "cluster", "schooludisecode", "schoolname")

  // User cache updater job metrics
  val userCacheHit = "user-cache-hit"
  val skipCount = "skipped-message-count"
  val successCount = "success-message-count"
  val dbReadSuccessCount = "db-read-success-count"
  val dbReadMissCount = "db-read-miss-count"
  val apiReadSuccessCount = "api-read-success-count"
  val apiReadMissCount = "api-read-miss-count"
  val totalEventsCount ="total-audit-events-count"

//  val userSelfSignedInTypeList: JList[String] = config.getStringList("user.self.signin.types")
//  val userValidatedTypeList: JList[String] = config.getStringList("user.validated.types")
//  val userSelfSignedKey: String = config.getString("user.self.signin.key")
//  val userValidatedKey: String = config.getString("user.valid.key")
//  val regdUserProducerPid: String = config.getString("regd.user.producer.pid")

  // Redis
//  val userStore: Int = config.getInt("redis-meta.database.userstore.id")

  val ratingParallelism: Int = config.getInt("task.rating.parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val issueEventSink = "rating-issue-event-sink"
  val issueOutputTagName = "rating-issue-events"
  val RAW_EVENTS_OUTPUT_TAG = "raw-events"

  val issueOutputTag: OutputTag[Event] = OutputTag[Event]("rated-events")

  // constants
  val userSignInTypeKey = "usersignintype"
  val userLoginTypeKey = "userlogintype"
  val firstName = "firstname"
  val lastName = "lastname"
  val rootOrgId = "rootorgid"
  val stateKey = "state"
  val districtKey = "district"
  val blockKey = "block"
  val clusterKey = "cluster"
  val orgnameKey = "orgname"
  val schoolKey = "school"
  val schoolUdiseCodeKey = "schooludisecode"
  val schoolNameKey = "schoolname"
  val `type` = "type"
  val subtype = "subType"
  val userTypeKey = "usertype"
  val userSubtypeKey = "usersubtype"
  val userId = "userid"
  val language = "language"
  val email = "email"
  val phone = "phone"

  //user store key prefix
  val userStoreKeyPrefix = "user:"
  // Consumers
  val RatingConsumer = "rating-consumer"

  // Functions
  val ratingFunction = "RatingFunction"

  //User Read API
//  val userReadApiUrl = config.getString("user-read.api.url")
//  val userReadApiFields = config.getString("user.read.url.fields")
//  val userReadApiErrors: JList[String] = config.getStringList("user.read.api.error")

//  val userRegistrationCountPath = config.getString("user.redis.registration-count.path")


}
