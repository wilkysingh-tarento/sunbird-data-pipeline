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

import java.util.{HashMap, UUID}
import scala.collection.mutable
import org.sunbird.dp.rating.task.RatingConfig
import com.google.gson.Gson
import java.util.UUID

import java.util
//import collection.mutable._

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
      logger.info("before the first query")

      val query = QueryBuilder.select().column("userid").from(config.dbKeyspace, config.courseTable)
        .where(QueryBuilder.eq(config.userId, "user1")).and(QueryBuilder.eq(config.courseId, event.activityId))
      val rows: java.util.List[Row] = cassandraUtil.find(query.toString);
      logger.info("after the first query  :  " + event.userId + "" + "" + event.activityId + "" + "" + event.activityType)
      if (null != rows && !rows.isEmpty) {
        logger.info("after the first query after null check  :  " + event.userId + "" + "" + event.activityId + "" + "" + event.activityType)
        logger.info("rows return : " + rows.get(0).toString)
        userStatus = true
        logger.info("before rating query")

        val ratingQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.ratingsTable)
          .where(QueryBuilder.eq(config.userId, event.userId)).
          and(QueryBuilder.eq(config.activityId, event.activityId)).
          and(QueryBuilder.eq(config.activityType, event.activityType))
        val ratingRows: java.util.List[Row] = cassandraUtil.find(ratingQuery.toString);
        val result = ratingRows.asScala.toList(0)
        logger.info("after rating query" + result.getFloat("rating"))

        if (null != ratingRows && !ratingRows.isEmpty) {
          logger.info("user already has a rating")
        } else {
          logger.info("user is newly adding rating")
        }
        context.output(config.issueOutputTag, event)

      }
      //       calculate delta and update the same in total ratings
      logger.info("delta check :" + event.updatedValues.get("rating").asInstanceOf[Double])

      //      logger.info("delta check :"+event.updatedValues.rating)

      val delta = event.updatedValues.get("rating").asInstanceOf[Double].toFloat - event.prevValues.get("rating").asInstanceOf[Double].toFloat
      val validReview = event.updatedValues.get("review").asInstanceOf[String]
      logger.info("delta and validReview are good  " + delta + "" + validReview)

      if (delta != 0.0f || (validReview.size > 100)) {
        var tempRow: Row = null

        logger.info("inside delta conditions")

        val ratingQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.ratingsSummaryTable) //table name =ratings_Summary
          .where(QueryBuilder.eq(config.activityId, event.activityId))
          .and(QueryBuilder.eq(config.activityType, event.activityType)).toString
        logger.info("delta calculation done")


        val ratingRows: java.util.List[Row] = cassandraUtil.find(ratingQuery.toString);
        logger.info("ratingROws : " + ratingRows)
        var updatedRating: Float = 0.0f
        var updatedRatingValues: HashMap[Float, Float] = new HashMap[Float, Float]()
        logger.info("completed updated rating value is  : " + updatedRatingValues)
        var prevRating: Float = 0
        if (null != ratingRows && !ratingRows.isEmpty) {
          tempRow = ratingRows.asScala.toList(0)
          logger.info("TempRow : " + tempRow)

          //if delta is 0 then It is not required to call below stuff
          if (delta != 0.0f) {
            prevRating = event.prevValues.get("rating").asInstanceOf[Double].toFloat
            logger.info("rating value : " + prevRating.toFloat)
          } //to be romoved
        } //to be romoved
        updatedRating = event.updatedValues.get("rating").asInstanceOf[Double].toFloat

        updatedRatingValues = update_ratings_count(tempRow, prevRating, updatedRating) //update_ratings_count update the camel casings for method names
        logger.info("updated rating values :  " + updatedRatingValues + "summary :" + tempRow.getString("latest50reviews"))
        updateDB(event, updatedRatingValues, tempRow.getFloat("sum_of_total_ratings"),
          tempRow.getFloat("total_number_of_ratings"),
          tempRow.getString("latest50reviews"))
        //                    }
        //
        //            if (validReview.size > 100) { //make one DB call
        //              updateDB(event, updatedRatingValues, tempRow.getString("sum_of_total_ratings").asInstanceOf[Float] + delta,
        //                tempRow.getString("total_number_of_ratings").asInstanceOf[Float] + 1,
        //                tempRow.getString("summary").asInstanceOf[String])
        //            }
        //          }
        //          else {
        //
        //            //insert for first time and check what happen when prev rating is not there so need to update the current rating instead of delta
        //            //val prevRating = event.prevValues.get("rating").asInstanceOf[Float]
        //            val updatedRating = event.updatedValues.get("rating").asInstanceOf[Float]
        //            val updatedRatingValues = update_ratings_count(tempRow, 0.0f, updatedRating)
        //
        //            updateDB(event, updatedRatingValues, tempRow.getString("sum_of_total_ratings").asInstanceOf[Float] +
        //              event.updatedValues.get("rating").asInstanceOf[Float],
        //              tempRow.getString("total_number_of_ratings").asInstanceOf[Float] + 1, //add 1
        //              tempRow.getString("summary").asInstanceOf[String]) //if summary >100 chars then
        //
        //          } // can make one DB call
        //        }
        //      }
        //    }


      }
    }catch {
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
        logger.info("started in update DB")
        val ratingDBResult = getRatingSummary(event)
        val validReview = event.updatedValues.get("review").asInstanceOf[String]
        logger.info("started in update DB and validReview is:  " + validReview)

        var updatedReviews = ""
        if (null == ratingDBResult) {
          logger.info("ratingDBResult is null")

          if (validReview.size > 100) {
            logger.info("before update_Top50_Review_Summary inside if")

            updatedReviews = update_Top50_Review_Summary("", event) //top50 reviews shub be the variable name
            saveRatingSummary(event, updatedRatingValues, "", sumOfTotalRating, totalRating)
          }

        }
        else {
          if (validReview.size > 100) {
            logger.info("before update_Top50_Review_Summary inside else" + summary)
            updatedReviews = update_Top50_Review_Summary(summary, event)
            logger.info("uodated reviews is " + updatedReviews)
            updateRatingSummary(event, updatedRatingValues, updatedReviews, sumOfTotalRating, totalRating)
          }
        }
        //one more condition is required to validate if rows is null
        if (null != getRatingLookUp(event)) {
          logger.info("before deleteRatingLookup")
          deleteRatingLookup(event)
        }
        logger.info("before saveRatingLookup")
        saveRatingLookup(event)
      }

      def update_ratings_count(tempRow: Row, prevRating: Float, updatedRating: Float): HashMap[Float, Float] = {
        logger.info("value check " + prevRating.floor + "" + updatedRating.floor + "" + tempRow.toString)
        var ratingMap: HashMap[Float, Float] = new HashMap()
        var x = 0;
        for (i <- 0 to 4) {
          ratingMap.put(i + 1, 0)
          logger.info("ratingMap in for loop : " + ratingMap)
        }

        logger.info("inside update ratings count")
        if (tempRow != null) {
          ratingMap.put(1, tempRow.getFloat("totalcount1stars"))
          ratingMap.put(2, tempRow.getFloat("totalcount2stars"))
          ratingMap.put(3, tempRow.getFloat("totalcount3stars"))
          ratingMap.put(4, tempRow.getFloat("totalcount4stars"))
          ratingMap.put(5, tempRow.getFloat("totalcount5stars"))
          logger.info("inside  end of if block")

        }

        val newRating = (updatedRating).floor
        val oldRating = (prevRating).floor
        logger.info("inside  second update ratings count " + newRating + "" + oldRating)

        if (ratingMap.containsKey(newRating) && newRating != oldRating) {
          ratingMap.put(newRating, ratingMap.get(newRating) + 1)
          logger.info("inside newrating and ratingMap check " + ratingMap)

        }
        if (prevRating != 0.0f) {
          if (ratingMap.containsKey(oldRating) && newRating != oldRating) {
            ratingMap.put(oldRating, ratingMap.get(oldRating) - 1)
            logger.info("inside oldrating and ratingMap check " + ratingMap)

          }
        }
        logger.info("reached end of update rating count" + ratingMap)

        ratingMap

      }

          def update_Top50_Review_Summary(summary: String, event: Event): String = {
            logger.info("inside start of update_Top50_Review_Summary")

            var gson: Gson = new Gson()
            var ratingQueue = mutable.Queue[RatingJson]()
            val updatedReviewSize = event.updatedValues.get("review").asInstanceOf[String].size
            if (updatedReviewSize > 100) {
              logger.info("inside if block of updatedReview")

              // Why we are using summary
              if (!summary.isEmpty) {
                logger.info("inside if block of summary is empty"+summary)

                var ratingJson: Array[RatingJson] = gson.fromJson(summary, classOf[Array[RatingJson]])
                logger.info("after ratingJson initialisation "+ratingJson)


                ratingJson.foreach(
                  row => {
                    ratingQueue.enqueue(row)
                    logger.info("ratingqueue inside for loop"+ratingQueue)
                  });
                if (ratingQueue.size >= 50) {
                  ratingQueue.dequeue()
                }
              }
              logger.info("printing queue and summary"+ratingQueue+""+summary)
              ratingQueue.enqueue(RatingJson("review",
                event.userId.asInstanceOf[String],
                event.updatedValues.get("updatedOn").asInstanceOf[String],
                event.updatedValues.get("rating").asInstanceOf[Double].toFloat,
                event.updatedValues.get("review").asInstanceOf[String]))
              logger.info("printing queue again"+ratingQueue.toString()+""+event.updatedValues.get("updatedOn").asInstanceOf[String])
              val finalResult = ratingQueue.toList

              logger.info("trying "+finalResult )

              logger.info("trying array"+finalResult.toArray )

              gson.toJson(finalResult.toArray)
            } else {
              gson.toJson(summary)

            }
          }

        def getRatingSummary(event: Event): Row = {
       logger.info("started in getratingsummary")
            val query = QueryBuilder.select.all()
              .from(config.dbKeyspace, config.ratingsSummaryTable).
              where(QueryBuilder.eq("activity_id", event.activityId))
              .and(QueryBuilder.eq("activity_type", event.activityType)).toString
            logger.info("completed query in getratingsummary")

            val row = cassandraUtil.findOne(query)
            logger.info("Successfully retrieved the rating for summary - activityId: "
              + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
              + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
            row
          }

          def getRatingLookUp(event: Event): Row = {
            logger.info("inside start of getRatingLookUp")
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
            logger.info("inside start of saveRatingSummary")
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

            logger.info("inside beginning of deleteRatingLookup ")
            val query = QueryBuilder.delete(config.dbKeyspace, config.ratingsLookupTable)
              .from(config.dbKeyspace, config.ratingsSummaryTable)
              .where(QueryBuilder.eq("activity_id", event.activityId))
              .and(QueryBuilder.eq("activity_type", event.activityType))
              .and(QueryBuilder.eq("rating", event.updatedValues.get("rating").asInstanceOf[Double].toFloat))
              .and(QueryBuilder.eq("updatedon", timeBasedUuid)).toString

            logger.info("inside beginning of deleteRatingLookup befor query exec : "+query)

            cassandraUtil.upsert(query)
            logger.info("Successfully saved the rating for lookup - activityId: "
              + event.activityId + " ,activityType: " + event.activityType + " ,userId: "
              + event.userId + "with " + "prevValues: " + event.prevValues + "and updateValues" + event.updatedValues)
          }

          def saveRatingLookup(event: Event): Unit = {
            val updatingTime = event.prevValues.get("updatedOn").asInstanceOf[String]
            val timeBasedUuid = UUID.fromString(updatingTime)
            logger.info("inside saveratinglookup")
            val query = QueryBuilder.insertInto(config.dbKeyspace, config.ratingsLookupTable)
              .value("activity_id", event.activityId)
              .value("activity_type", event.activityType)
              .value("rating", event.updatedValues.get("rating").asInstanceOf[Double].toFloat)
              .value("updatedon", timeBasedUuid)
              .value("userid", event.userId).toString
            logger.info("saving in saveratinglookup"+query)

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
        logger.info("strated in updateRatingSummary "+event.activityId+""+event.activityType)
            logger.info("updated rating valus is :   "+updatedRatingValues)
            logger.info("updated rating valus is :   "+updatedRatingValues)
            val test =updatedRatingValues.get(1.0f)

            logger.info("updated rating valus is :   "+updatedRatingValues.get(1.0f)+"test"+test)

            val updateQuery = QueryBuilder.update(config.dbKeyspace, config.ratingsSummaryTable)
              .`with`(QueryBuilder.set("latest50reviews",summary))
              .and(QueryBuilder.set("sum_of_total_ratings", sumOfTotalRating))
              .and(QueryBuilder.set("total_number_of_ratings", sumOfTotalRating))
              .and(QueryBuilder.set("totalcount1stars", updatedRatingValues.get(1.0f)))
              .and(QueryBuilder.set("totalcount2stars", updatedRatingValues.get(2.0f)))
              .and(QueryBuilder.set("totalcount3stars", updatedRatingValues.get(3.0f)))
              .and(QueryBuilder.set("totalcount4stars", updatedRatingValues.get(4.0f)))
              .and(QueryBuilder.set("totalcount5stars", updatedRatingValues.get(5.0f)))
              .where(QueryBuilder.eq("activity_id", event.activityId))
              .and(QueryBuilder.eq("activity_type", event.activityType))
            logger.info("added query in updateRatingSummary  "+updateQuery.toString)

            cassandraUtil.upsert(updateQuery.toString)
            logger.info("Successfully updated ratings in rating summary  - activity_id: "
              + event.activityId + " ,activity_type: " + event.activityType)
      }
}
