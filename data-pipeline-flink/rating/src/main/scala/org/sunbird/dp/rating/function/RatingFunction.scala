package org.sunbird.dp.rating.function

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.DataCache
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.{CassandraUtil, JSONUtil}
import org.sunbird.dp.rating.domain.Event
import org.sunbird.dp.rating.task.RatingConfig

import java.util.{HashMap, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable

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
  }

  case class RatingJson(objectType: String, var user_id: String, var date: String, var rating: Float, var review: String)

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    var userStatus: Boolean = false
    try {
      val query = QueryBuilder.select().column("userid").from(config.dbKeyspace, config.courseTable)
        .where(QueryBuilder.eq(config.userId, "user1")).and(QueryBuilder.eq(config.courseId, event.activityId))
      val rows: java.util.List[Row] = cassandraUtil.find(query.toString);
      if (null != rows && !rows.isEmpty) {
        userStatus = true
        val ratingQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.ratingsTable)
          .where(QueryBuilder.eq(config.userId, event.userId)).
          and(QueryBuilder.eq(config.activityId, event.activityId)).
          and(QueryBuilder.eq(config.activityType, event.activityType))
        val ratingRows: java.util.List[Row] = cassandraUtil.find(ratingQuery.toString);
        val result = ratingRows.asScala.toList(0)

        if (null != ratingRows && !ratingRows.isEmpty) {
          logger.info("user already has a rating")
        } else {
          logger.info("user is newly adding rating")
        }
        context.output(config.issueOutputTag, event)

      }
      //       calculate delta and update the same in total ratings

      var delta = event.updatedValues.get("rating").asInstanceOf[Double].toFloat - event.prevValues.get("rating").asInstanceOf[Double].toFloat
      val validReview = event.updatedValues.get("review").asInstanceOf[String]
      var tempRow: Row = null

      var sumOfTotalRatings = tempRow.getString("sum_of_total_ratings").asInstanceOf[Float]
      var totalNumberOfRatings = tempRow.getString("total_number_of_ratings").asInstanceOf[Float]
      var updatedRatingValues: HashMap[Float, Float] = new HashMap[Float, Float]()
      var reviewsExist : String = ""
      if (delta != 0 || (validReview.size > 100)) {
        var prevRating: Float = 0.0f
        var counter: Integer = 0
        val ratingQuery = QueryBuilder.select().column("*").from(config.dbKeyspace, config.ratingsSummaryTable) //table name =ratings_Summary
          .where(QueryBuilder.eq(config.activityId, event.activityId))
          .and(QueryBuilder.eq(config.activityType, event.activityType)).toString


        val ratingRows: java.util.List[Row] = cassandraUtil.find(ratingQuery.toString);
        var updatedRating: Float = 0.0f
        updatedRating = event.updatedValues.get("rating").asInstanceOf[Float]
        if(!tempRow.getString("latest50reviews").equals(null)||tempRow.getString("latest50reviews").isEmpty){
          reviewsExist = tempRow.getString("latest50reviews")
        }
        if (null != ratingRows && !ratingRows.isEmpty) {
          tempRow = ratingRows.asScala.toList(0)
          //if delta is 0 then It is not required to call below stuff
          if (delta != 0) {
            sumOfTotalRatings= sumOfTotalRatings+delta
            prevRating = event.prevValues.get("rating").asInstanceOf[Float]
            updatedRatingValues = update_ratings_count(tempRow, prevRating, updatedRating) //update_ratings_count update the camel casings for method names
            delta = 0
          }

          if (validReview.size > 100 && delta==0) { //make one DB call

            updatedRatingValues = update_ratings_count(tempRow, prevRating, 0.0f) //update_ratings_count update the camel casings for method names
            counter = 1

          }
        }
        else {

          //insert for first time and check what happen when prev rating is not there so need to update the current rating instead of delta
          //val prevRating = event.prevValues.get("rating").asInstanceOf[Float]
          updatedRatingValues = update_ratings_count(tempRow, 0.0f, updatedRating) //check condition when tempRow is null
          sumOfTotalRatings = sumOfTotalRatings+delta
          totalNumberOfRatings=totalNumberOfRatings+1

        } // can make one DB call
      }
      updateDB(event, updatedRatingValues, sumOfTotalRatings,
        totalNumberOfRatings,
        reviewsExist)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        println(ex)
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
    if (null == ratingDBResult) {

      if (validReview.size > 100) {

        updatedReviews = update_Top50_Review_Summary("", event) //top50 reviews shub be the variable name
        saveRatingSummary(event, updatedRatingValues, "", sumOfTotalRating, totalRating)
      }

    }
    else {
      if (validReview.size > 100) {
        updatedReviews = update_Top50_Review_Summary(summary, event)
        updateRatingSummary(event, updatedRatingValues, updatedReviews, sumOfTotalRating, totalRating)
      }
    }
    //one more condition is required to validate if rows is null
    if (null != getRatingLookUp(event)) {
      deleteRatingLookup(event)
    }
    saveRatingLookup(event)
  }

  def update_ratings_count(tempRow: Row, prevRating: Float, updatedRating: Float): HashMap[Float, Float] = {
    var ratingMap: HashMap[Float, Float] = new HashMap()
    var x = 0;
    for (i <- 0 to 4) {
      ratingMap.put(i + 1, 0)
    }

    if (tempRow != null) {
      ratingMap.put(1, tempRow.getFloat("totalcount1stars"))
      ratingMap.put(2, tempRow.getFloat("totalcount2stars"))
      ratingMap.put(3, tempRow.getFloat("totalcount3stars"))
      ratingMap.put(4, tempRow.getFloat("totalcount4stars"))
      ratingMap.put(5, tempRow.getFloat("totalcount5stars"))

    }

    val newRating = (updatedRating).floor
    val oldRating = (prevRating).floor

    if (ratingMap.containsKey(newRating) && newRating != oldRating) {
      ratingMap.put(newRating, ratingMap.get(newRating) + 1)

    }
    if (prevRating != 0.0f) {
      if (ratingMap.containsKey(oldRating) && newRating != oldRating) {
        ratingMap.put(oldRating, ratingMap.get(oldRating) - 1)

      }
    }

    ratingMap

  }

  def update_Top50_Review_Summary(summary: String, event: Event): String = {

    var gson: Gson = new Gson()
    var ratingQueue = mutable.Queue[RatingJson]()
    val updatedReviewSize = event.updatedValues.get("review").asInstanceOf[String].size
    if (updatedReviewSize > 100) {

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
        event.updatedValues.get("updatedOn").asInstanceOf[String],
        event.updatedValues.get("rating").asInstanceOf[Double].toFloat,
        event.updatedValues.get("review").asInstanceOf[String]))
      val finalResult = ratingQueue.toList


      gson.toJson(finalResult.toArray)
    } else {
      gson.toJson(summary)

    }
  }

  def getRatingSummary(event: Event): Row = {
    val query = QueryBuilder.select.all()
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
    val query = QueryBuilder.select.all()
      .from(config.dbKeyspace, config.ratingsLookupTable).
      where(QueryBuilder.eq("activity_id", event.activityId))
      .and(QueryBuilder.eq("activity_type", event.activityType))
      .and(QueryBuilder.eq("rating", event.prevValues.get("rating").asInstanceOf[Double].toFloat)).toString

    val row = cassandraUtil.findOne(query)
    logger.info("Successfully retrieved the rating for summary - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
    row
  }

  def saveRatingSummary(event: Event, updatedRatingValues: HashMap[Float, Float],
                        summary: String, sumOfTotalRating: Float, totalRating: Float): Unit = {
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsSummaryTable)
      .value("activity_id", event.activityId)
      .value("activity_type", event.activityType)
      .value("latest50reviews", summary)
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
    val updatingTime = event.prevValues.get("updatedOn").asInstanceOf[String]
    val timeBasedUuid = UUID.fromString(updatingTime)

    val query = QueryBuilder.delete(config.dbKeyspace, config.ratingsLookupTable)
      .from(config.dbKeyspace, config.ratingsSummaryTable)
      .where(QueryBuilder.eq("activity_id", event.activityId))
      .and(QueryBuilder.eq("activity_type", event.activityType))
      .and(QueryBuilder.eq("rating", event.updatedValues.get("rating").asInstanceOf[Double].toFloat))
      .and(QueryBuilder.eq("updatedon", timeBasedUuid)).toString


    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def saveRatingLookup(event: Event): Unit = {
    val updatingTime = event.prevValues.get("updatedOn").asInstanceOf[String]
    val timeBasedUuid = UUID.fromString(updatingTime)
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsLookupTable)
      .value("activity_id", event.activityId)
      .value("activity_type", event.activityType)
      .value("rating", event.updatedValues.get("rating").asInstanceOf[Double].toFloat)
      .value("updatedon", timeBasedUuid)
      .value("userid", event.userId).toString

    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def createRatingLookup(event: Event): Unit = {
    val updatingTime = event.prevValues.get("updatedOn").asInstanceOf[String]
    val timeBasedUuid = UUID.fromString(updatingTime)
    val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsLookupTable)
      .value("activity_id", event.activityId)
      .value("activity_type", event.activityType)
      .value("rating", event.updatedValues.get("rating").asInstanceOf[Float])
      .value("updatedon", timeBasedUuid)
      .value("user_id", event.userId).toString

    cassandraUtil.upsert(query)
    logger.info("Successfully saved the rating for lookup - activityId: "
      + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
      + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
  }

  def updateRatingSummary(event: Event, updatedRatingValues: HashMap[Float, Float],
                          summary: String, sumOfTotalRating: Float, totalRating: Float): Unit = {
    val test = updatedRatingValues.get(1.0f)

    val updateQuery = QueryBuilder.update(config.dbKeyspace, config.ratingsSummaryTable)
      .`with`(QueryBuilder.set("latest50reviews", summary))
      .and(QueryBuilder.set("sum_of_total_ratings", sumOfTotalRating))
      .and(QueryBuilder.set("total_number_of_ratings", sumOfTotalRating))
      .and(QueryBuilder.set("totalcount1stars", updatedRatingValues.get(1.0f)))
      .and(QueryBuilder.set("totalcount2stars", updatedRatingValues.get(2.0f)))
      .and(QueryBuilder.set("totalcount3stars", updatedRatingValues.get(3.0f)))
      .and(QueryBuilder.set("totalcount4stars", updatedRatingValues.get(4.0f)))
      .and(QueryBuilder.set("totalcount5stars", updatedRatingValues.get(5.0f)))
      .where(QueryBuilder.eq("activity_id", event.activityId))
      .and(QueryBuilder.eq("activity_type", event.activityType))
    cassandraUtil.upsert(updateQuery.toString)
    logger.info("Successfully updated ratings in rating summary  - activity_id: "
      + event.activityId + " ,activity_type: " + event.activityType)
  }
}
