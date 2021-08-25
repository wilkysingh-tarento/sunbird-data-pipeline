package org.sunbird.dp.cbpreprocessor.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.sunbird.dp.cbpreprocessor.domain.Event
import org.sunbird.dp.cbpreprocessor.task.CBPreprocessorConfig

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

// import com.google.gson.Gson
// import org.sunbird.dp.core.util.JSONUtil
// import com.google.gson.Gson
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.dp.core.job.Metrics
// import scala.util.Try

/**
 * util for flattening cb_audit work order events
 */
class CBEventsFlattenerUtil extends java.io.Serializable {

  private val serialVersionUID = 1167435095740381669L  // TODO: update?
  val objectMapper = new ObjectMapper()

  case class MapMergeParams(map: util.Map[String, Any], prefix: String, exclusions: List[String])

  /**
   * merge arbitrary number of maps and return a new map
   * `mergedMap(List(MapMergeParams(map, "", exclusions)))` returns a shallow copy of `map` excluding `exclusions`
   *
   * @param mergeParamList
   * @return
   */
  def mergedMap(mergeParamList: List[MapMergeParams]): util.Map[String, Any] = {
    val newMap = new util.HashMap[String, Any]()
    mergeParamList.foreach(params => {
      val exclusions = if (params.exclusions.isEmpty) Set() else params.exclusions.toSet
      val fieldsToCopy = params.map.keySet().toArray.toSet - exclusions
      fieldsToCopy.asInstanceOf[Set[String]].foreach(key => {
        val mergedKey = if (params.prefix != "") key else params.prefix + key
        newMap.put(mergedKey, params.map.get(key))
      })
    })
    newMap
  }

  /**
   * return a list of flattened (denormalized) maps from workOrderMap
   *
   * @param workOrderMap
   * @return
   */
  def flattenWorkOrderMap(workOrderMap: util.Map[String, Any]): util.ArrayList[util.Map[String, Any]] = {
    val flattenedList = new util.ArrayList[util.Map[String, Any]]()
    // TO-MERGE: workOrderMap, prefix=None, exclude=['users']
    val users = workOrderMap.get("users").asInstanceOf[util.ArrayList[util.Map[String, Any]]]
    users.forEach(user => {
      // TO-MERGE: user, prefix='users_', exclude=['roleCompetencyList']
      val roleCompetencyList = user.get("roleCompetencyList").asInstanceOf[util.ArrayList[util.Map[String, Any]]]
      roleCompetencyList.forEach(roleCompetency => {
        // TO-MERGE: roleCompetency, prefix='users_roleCompetencyList_', exclude=['roleDetails','competencyDetails']
        // exclude=['roleDetails','competencyDetails'] results in no fields from this level being merged
        val roleDetails = roleCompetency.get("roleDetails").asInstanceOf[util.Map[String, Any]]
        // TO-MERGE: roleDetails, prefix='users_roleCompetencyList_roleDetails_', exclude=['childNodes']

        val competencyDetails = roleCompetency.get("competencyDetails").asInstanceOf[util.ArrayList[util.Map[String, Any]]]
        competencyDetails.forEach(competency => {
          // TO-MERGE: competency, prefix='users_roleCompetencyList_competencyDetails_', exclude=[]
          val newMap = mergedMap(List(
            //             Map              Prefix                                          Exclusion
            MapMergeParams(workOrderMap,    "",                                             List("users")),
            MapMergeParams(user,            "users_",                                       List("roleCompetencyList")),
            MapMergeParams(roleCompetency,  "users_roleCompetencyList_",                    List("roleDetails", "competencyDetails")),
            MapMergeParams(roleDetails,     "users_roleCompetencyList_roleDetails_",        List("childNodes")),
            MapMergeParams(competency,      "users_roleCompetencyList_competencyDetails_",  List())
          ))
          flattenedList.add(newMap)
        })

        val roleChildNodes = roleDetails.get("childNodes").asInstanceOf[util.ArrayList[util.Map[String, Any]]]
        roleChildNodes.forEach(childNode => {
          // TO-MERGE: childNode, prefix='users_roleCompetencyList_roleDetails_childNodes_', exclude=[]
          val newMap = mergedMap(List(
            //             Map              Prefix                                              Exclusion
            MapMergeParams(workOrderMap,    "",                                                 List("users")),
            MapMergeParams(user,            "users_",                                           List("roleCompetencyList")),
            MapMergeParams(roleCompetency,  "users_roleCompetencyList_",                        List("roleDetails", "competencyDetails")),
            MapMergeParams(roleDetails,     "users_roleCompetencyList_roleDetails_",            List("childNodes")),
            MapMergeParams(childNode,       "users_roleCompetencyList_roleDetails_childNodes_", List()),
          ))
          flattenedList.add(newMap)
        })
      })
    })
    flattenedList
  }

  /**
   * flatten work order data in `event` and return a buffer of CB_AUDIT_ITEM events
   *
   * @param event
   * @return
   */
  def flattenedEvents(event: Event): mutable.Buffer[Event] = {
    val workOrderMap = event.cbData
    val eventMap = event.getMap()
    flattenWorkOrderMap(workOrderMap).asScala.map(flatWorkOrderMap => {
      val eventMapEData = eventMap.get("edata").asInstanceOf[util.Map[String, Any]]
      // create a shallow copy of eventMap excluding edata
      val newEventMap = mergedMap(List(MapMergeParams(eventMap, "", List("edata"))))
      // store parent mid as parent_mid
      newEventMap.put("parent_mid", newEventMap.get("mid"))
      // generate new mid
      newEventMap.put("mid", "CB_AUDIT_ITEM:" + util.UUID.randomUUID().toString)
      // change eid to CB_AUDIT_ITEM
      newEventMap.put("eid", "CB_AUDIT_ITEM")
      // create a shallow copy of eventMapEData excluding cb_data
      val newEventMapEData = mergedMap(List(MapMergeParams(eventMapEData, "", List("cb_data"))))
      // update new edata, put flatWorkOrderMap as cb_data
      newEventMapEData.put("cb_data", flatWorkOrderMap)
      // update new event map, put newEventMapEData as edata
      newEventMap.put("edata", newEventMapEData)
      new Event(newEventMap)
    })
  }

}
