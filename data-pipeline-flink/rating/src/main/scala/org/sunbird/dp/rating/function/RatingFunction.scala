package org.sunbird.dp.rating.function

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointStatisticsWithSubtaskDetails.Summary
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.{CassandraUtil, JSONUtil}
import org.sunbird.dp.rating.domain.Event

import scala.collection.mutable
import org.sunbird.dp.rating.task.RatingConfig
import com.google.gson.Gson
import collection.mutable._

import scala.collection.JavaConverters._

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

  case class RatingJson(objectType: String, var user_id: String, var date: String, var rating: Float, var review: String)

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    var userStatus: Boolean = false
    try {
      //does user enrolled for a course

      val query = QueryBuilder.select().column("*").from(config.dbKeyspace, config.courseTable)
        .where(QueryBuilder.eq(config.userId, event.userId)).and(QueryBuilder.eq("active", "YES"))
      val rows: java.util.List[Row] = cassandraUtil.find(query.toString);
      if (null != rows && !rows.isEmpty) {

        userStatus = true
        val ratingQuery = QueryBuilder.select().column("*").from(config.dbKeyspace, config.ratingsTable)
          .where(QueryBuilder.eq(config.userId, event.userId))
        val ratingRows: java.util.List[Row] = cassandraUtil.find(ratingQuery.toString);
        if (null != ratingRows && !ratingRows.isEmpty) {
          println("user already has a rating")
        } else {
          println("user is newly adding rating")
        }
        context.output(config.issueOutputTag, event)


        // calculate delta and update the same in total ratings

        val delta = event.updatedValues.get("rating").asInstanceOf[Float] - event.prevValues.get("rating").asInstanceOf[Float]
        val validReview = event.updatedValues.get("review").asInstanceOf[String]

        if (delta != 0 || (validReview.size > 100)) {
          var tempRow: Row = new Row {}

          val ratingQuery = QueryBuilder.select().column("*").from(config.dbKeyspace, config.ratingsSummaryTable) //table name =ratings_Summary
            .where(QueryBuilder.eq(config.activityId, event.activityId))
            .and(QueryBuilder.eq(config.activityType, event.activityType)).toString


          val ratingRows: java.util.List[Row] = cassandraUtil.find(ratingQuery.toString);
          var updatedRating :Float = 0.0f
          var updatedRatingValues: HashMap[Float, Float] = HashMap[Float, Float]()
          if (null != ratingRows && !ratingRows.isEmpty) {
            tempRow = ratingRows.asScala.toList(0)
            //if delta is 0 then It is not required to call below stuff
            if(delta != 0) {
              val prevRating = event.prevValues.get("rating").asInstanceOf[Float]
              updatedRating = event.updatedValues.get("rating").asInstanceOf[Float]
              updatedRatingValues = update_ratings_count(tempRow, prevRating, updatedRating) //update_ratings_count update the camel casings for method names

              updateDB(event, updatedRatingValues, tempRow.getString("sum_of_total_ratings").asInstanceOf[Float] ,
                tempRow.getString("total_number_of_ratings").asInstanceOf[Float],
                tempRow.getString("summary").asInstanceOf[String])
            }

               if(validReview.size > 100) {   //make one DB call
                 updateDB(event, updatedRatingValues, tempRow.getString("sum_of_total_ratings").asInstanceOf[Float] + delta,
                   tempRow.getString("total_number_of_ratings").asInstanceOf[Float] + 1,
                   tempRow.getString("summary").asInstanceOf[String])
               }
          }
          else {

            //insert for first time and check what happen when prev rating is not there so need to update the current rating instead of delta
            //val prevRating = event.prevValues.get("rating").asInstanceOf[Float]
            val updatedRating = event.updatedValues.get("rating").asInstanceOf[Float]
            val updatedRatingValues = update_ratings_count(tempRow, 0.0f, updatedRating)

            updateDB(event, updatedRatingValues, tempRow.getString("sum_of_total_ratings").asInstanceOf[Float] +
              event.updatedValues.get("rating").asInstanceOf[Float],
              tempRow.getString("total_number_of_ratings").asInstanceOf[Float] + 1, //add 1
              tempRow.getString("summary").asInstanceOf[String]) //if summary >100 chars then 

          } // can make one DB call
        }
      }
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

  def updateDB(event: Event, updatedRatingValues: HashMap[Float, Float],
               sumOfTotalRating: Float, totalRating: Float,
               summary: String): Unit = {
    val ratingDBResult = getRatingSummary(event)
    val validReview = event.updatedValues.get("review").asInstanceOf[String]
    var updatedReviews = ""
    //    if(validReview.size > 100) {
    if (null == ratingDBResult) {
      if (validReview.size > 100) {
        updatedReviews = update_Top50_Review_Summary("", event) //top50 reviews shub be the variable name
      }
      saveRatingSummary(event, updatedRatingValues, "", sumOfTotalRating, totalRating)

    }
    else {
      updatedReviews = update_Top50_Review_Summary(summary, event)
      updateRatingSummary(event, updatedRatingValues, updatedReviews, sumOfTotalRating, totalRating)

    }
    //one more condition is required to validate if rows is null
    if (null != getRatingLookUp(event)) {
      deleteRatingLookup(event)
    }

    saveRatingLookup(event)
  }

  def update_ratings_count(tempRow: Row, prevRating: Float, updatedRating: Float): HashMap[Float, Float] = {

    var ratingMap: HashMap[Float, Float] = new HashMap()
    ratingMap.put(1, tempRow.getFloat("totalcount1stars"))
    ratingMap.put(2, tempRow.getFloat("totalcount2stars"))
    ratingMap.put(3, tempRow.getFloat("totalcount3stars"))
    ratingMap.put(4, tempRow.getFloat("totalcount4stars"))
    ratingMap.put(5, tempRow.getFloat("totalcount5stars"))

    val newRating = (updatedRating).floor

    if (ratingMap.contains(newRating)) {
      ratingMap.put(newRating, ratingMap(newRating) + 1)
    }
    else if (prevRating != 0.0f) {
      val oldRating = (prevRating).floor
      if (ratingMap.contains(oldRating)) {
        ratingMap.put(oldRating, ratingMap(oldRating) - 1)
      }
    }
    ratingMap

  }

  def update_Top50_Review_Summary(summary: String, event: Event): String = {
    var gson: Gson = new Gson()
    var ratingQueue = Queue[RatingJson]()
    val updatedReview = event.updatedValues.get("review").asInstanceOf[String].size
    if (updatedReview > 100) {
      // Why we are using summary
      if (!summary.isEmpty) {
        var ratingJson: Array[RatingJson] = gson.fromJson(summary, classOf[Array[RatingJson]])


        ratingJson.foreach(
          row => {
            ratingQueue.enqueue(row)
          });
        if (ratingQueue.size >= 50) {
          ratingQueue.dequeue()
        }
      }
      ratingQueue.enqueue(RatingJson("review",
        event.userId.asInstanceOf[String],
        event.updatedValues.get("updatedon").asInstanceOf[String],
        event.updatedValues.get("rating").asInstanceOf[Float],
        event.updatedValues.get("review").asInstanceOf[String]))
      gson.toJson(ratingQueue)
    } else {
      summary
    }
  }


  def getRatingSummary(event: Event): Row = {

    val query = QueryBuilder.select("*")
      .from(config.dbKeyspace, config.ratingsSummaryTable).
      where(QueryBuilder.eq("activity_id", event.activityId))
      .and(QueryBuilder.eq("activity_type", event.activityType)).toString

    val row = cassandraUtil.findOne(query)
    logger.info("Successfully retrieved the rating for summary - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
    row
  }

  def getRatingLookUp(event: Event): Row = {

    val query = QueryBuilder.select("*")
      .from(config.dbKeyspace, config.ratingsSummaryTable).
      where(QueryBuilder.eq("activity_id", event.activityId))
      .and(QueryBuilder.eq("activity_type", event.activityType))
      .and(QueryBuilder.eq("rating", event.prevValues.get("rating").asInstanceOf[Float])).toString

    val row = cassandraUtil.findOne(query)
    logger.info("Successfully retrieved the rating for summary - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
    row
  }

  def saveRatingSummary(event: Event, updatedRatingValues: HashMap[Float, Float],
                        summary: String, sumOfTotalRating: Float, totalRating: Float): Unit = {
    val updatedSummary = update_Top50_Review_Summary(summary: String, event) //: Queue[RatingJson]
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsSummaryTable)
      .value("activity_id", event.activityId)
      .value("activity_type", event.activityType)
      .value("latest50reviews", updatedSummary)
      .value("sum_of_total_ratings", sumOfTotalRating)
      .value("total_number_of_ratings", totalRating)
      .value("totalcount1stars", updatedRatingValues.get(1))
      .value("totalcount2stars", updatedRatingValues.get(2))
      .value("totalcount3stars", updatedRatingValues.get(3))
      .value("totalcount4stars", updatedRatingValues.get(4))
      .value("totalcount5stars", updatedRatingValues.get(5)).toString


    cassandraUtil.upsert(query)
    logger.info("Successfully processed the rating event - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def deleteRatingLookup(event: Event): Unit = {
    val query = QueryBuilder.delete(config.dbKeyspace, config.ratingsLookupTable)
      .value("activity_id", event.activityId)
      .value("activity_type", event.activityType)
      .value("rating", event.updatedValues.get("rating").asInstanceOf[Float])
      .value("updatedon", event.prevValues.get("updatedOn").asInstanceOf[String]).toString

    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def saveRatingLookup(event: Event): Unit = {
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsLookupTable)
      .value("activity_id", event.activityId)
      .value("activity_type", event.activityType)
      .value("rating", event.updatedValues.get("rating").asInstanceOf[Float])
      .value("updatedon", event.updatedValues.get("updatedOn").asInstanceOf[String])
      .value("user_id", event.userId).toString

    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def createRatingLookup(event: Event): Unit = {
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsLookupTable)
      .value("activity_id", event.activityId)
      .value("activity_type", event.activityType)
      .value("rating", event.updatedValues.get("rating").asInstanceOf[Float])
      .value("updatedon", event.updatedValues.get("updatedOn").asInstanceOf[String])
      .value("user_id", event.userId).toString

    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def updateRatingSummary(event: Event, updatedRatingValues: HashMap[Float, Float],
                          summary: String, sumOfTotalRating: Float, totalRating: Float): Unit = {
    val updatedSummary=update_Top50_Review_Summary(summary: String, event) //: Queue[RatingJson]

    val updateQuery = QueryBuilder.update(config.dbKeyspace, config.ratingsSummaryTable)
      .`with`(QueryBuilder.set("latest50reviews", updatedSummary))
      .and(QueryBuilder.set("sum_of_total_ratings", sumOfTotalRating))
      .and(QueryBuilder.set("total_number_of_ratings", sumOfTotalRating))
      .and(QueryBuilder.set("totalcount1stars", updatedRatingValues.get(1)))
      .and(QueryBuilder.set("totalcount2stars", updatedRatingValues.get(2)))
      .and(QueryBuilder.set("totalcount3stars", updatedRatingValues.get(3)))
      .and(QueryBuilder.set("totalcount4stars", updatedRatingValues.get(4)))
      .and(QueryBuilder.set("totalcount5stars", updatedRatingValues.get(5)))
      .where(QueryBuilder.eq("activity_id", event.activityId))
      .and(QueryBuilder.eq("activity_type", event.activityType))

    cassandraUtil.upsert(updateQuery.toString)
    logger.info("Successfully updated ratings in rating summary  - activity_id: "
      + event.activityId + " ,activity_type: " + event.activityType )
  }

}

