package org.sunbird.dp.cbpreprocessor.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.sunbird.dp.cbpreprocessor.domain.Event
import org.sunbird.dp.cbpreprocessor.task.CBPreprocessorConfig

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// import com.google.gson.Gson
// import org.sunbird.dp.core.util.JSONUtil
// import com.google.gson.Gson
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.dp.core.job.Metrics
// import scala.util.Try

/**
 * util for flattening cb_audit work order events
 */
class CBEventsFlattener extends java.io.Serializable {

  private val serialVersionUID = 1167435095740381669L  // TODO: update?

  private case class MapMergeParams(map: util.Map[String, Any], prefix: String, exclusions: List[String])
  private case class WorkOrderMapData(map: util.Map[String, Any], childType: String, hasRole: Boolean)


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
  def flattenWorkOrderEventData(workOrderMap: util.Map[String, Any]): List[WorkOrderMapData] = {
    val flattenedList = ListBuffer[WorkOrderMapData]()
    // TO-MERGE: workOrderMap, prefix=None, exclude=['users']
    val users = workOrderMap.get("users").asInstanceOf[util.ArrayList[util.Map[String, Any]]]

    // if users is empty, TODO: handle this for all empty lists
    if (users.isEmpty) {
      val newMap = mergedMap(List(MapMergeParams(workOrderMap, "", List("users"))))
      flattenedList.append(WorkOrderMapData(newMap, "", hasRole = false))
      return flattenedList.toList
    }

    users.forEach(user => {
      // TO-MERGE: user, prefix='wa_', exclude=['roleCompetencyList', 'unmappedActivities', 'unmappedCompetencies']

      // take care of unmappedActivities
      val unmappedActivities = user.get("unmappedActivities").asInstanceOf[util.ArrayList[util.Map[String, Any]]]
      unmappedActivities.forEach(activity => {
        // TO-MERGE: activity, prefix='wa_activity_', exclude=[]
        val newMap = mergedMap(List(
          //             Map              Prefix            Exclusion
          MapMergeParams(workOrderMap,    "",               List("users")),
          MapMergeParams(user,            "wa_",            List("roleCompetencyList", "unmappedActivities", "unmappedCompetencies")),
          MapMergeParams(activity,        "wa_activity_",   List()),
        ))
        flattenedList.append(WorkOrderMapData(newMap, "activity", hasRole = false))
      })

      // take care of unmappedCompetencies
      val unmappedCompetencies = user.get("unmappedCompetencies").asInstanceOf[util.ArrayList[util.Map[String, Any]]]
      unmappedCompetencies.forEach(competency => {
        // TO-MERGE: competency, prefix='wa_competency_', exclude=[]
        val newMap = mergedMap(List(
          //             Map            Prefix              Exclusion
          MapMergeParams(workOrderMap,  "",                 List("users")),
          MapMergeParams(user,          "wa_",              List("roleCompetencyList", "unmappedActivities", "unmappedCompetencies")),
          MapMergeParams(competency,    "wa_competency_",   List()),
        ))
        flattenedList.append(WorkOrderMapData(newMap, "competency", hasRole = false))
      })

      // dive into roleCompetencyList
      val roleCompetencyList = user.get("roleCompetencyList").asInstanceOf[util.ArrayList[util.Map[String, Any]]]
      roleCompetencyList.forEach(roleCompetency => {
        // TO-MERGE: roleCompetency, prefix='wa_rcl_', exclude=['roleDetails','competencyDetails']
        // exclude=['roleDetails','competencyDetails'] results in no fields from this level being merged
        val roleDetails = roleCompetency.get("roleDetails").asInstanceOf[util.Map[String, Any]]
        // TO-MERGE: roleDetails, prefix='wa_role_', exclude=['childNodes']

        // activities associated with this role
        val roleChildNodes = roleDetails.get("childNodes").asInstanceOf[util.ArrayList[util.Map[String, Any]]]
        roleChildNodes.forEach(childNode => {
          // TO-MERGE: childNode, prefix='wa_activity_', exclude=[]
          val newMap = mergedMap(List(
            //             Map              Prefix            Exclusion
            MapMergeParams(workOrderMap,    "",               List("users")),
            MapMergeParams(user,            "wa_",            List("roleCompetencyList", "unmappedActivities", "unmappedCompetencies")),
            MapMergeParams(roleCompetency,  "wa_rcl_",        List("roleDetails", "competencyDetails")),
            MapMergeParams(roleDetails,     "wa_role_",       List("childNodes")),
            MapMergeParams(childNode,       "wa_activity_",   List()),
          ))
          flattenedList.append(WorkOrderMapData(newMap, "activity", hasRole = true))
        })

        // competencies associated with this role
        val competencyDetails = roleCompetency.get("competencyDetails").asInstanceOf[util.ArrayList[util.Map[String, Any]]]
        competencyDetails.forEach(competency => {
          // TO-MERGE: competency, prefix='wa_competency_', exclude=[]
          val newMap = mergedMap(List(
            //             Map              Prefix             Exclusion
            MapMergeParams(workOrderMap,    "",                List("users")),
            MapMergeParams(user,            "wa_",             List("roleCompetencyList", "unmappedActivities", "unmappedCompetencies")),
            MapMergeParams(roleCompetency,  "wa_rcl_",         List("roleDetails", "competencyDetails")),
            MapMergeParams(roleDetails,     "wa_role_",        List("childNodes")),
            MapMergeParams(competency,      "wa_competency_",  List())
          ))
          flattenedList.append(WorkOrderMapData(newMap, "competency", hasRole = true))
        })
      })
    })
    flattenedList.toList
  }

  /**
   * flatten work order data in `event` and return a Seq of CB_AUDIT_ITEM events
   *
   * @param event
   * @return
   */
  def flattenedEvents(event: Event): Seq[(Event, String, Boolean)] = {
    val workOrderMap = event.cbData.get("data").asInstanceOf[util.Map[String, Any]]
    val eventMap = event.getMap()
    flattenWorkOrderEventData(workOrderMap).map(flatEventData => {
      val eventMapEData = eventMap.get("edata").asInstanceOf[util.Map[String, Any]]
      // create a shallow copy of eventMap excluding edata
      val newEventMap = mergedMap(List(MapMergeParams(eventMap, "", List("edata"))))
      // store parent mid as parent_mid
      newEventMap.put("mid_parent", newEventMap.get("mid"))
      // generate new mid
      newEventMap.put("mid", "CB_ITEM:" + util.UUID.randomUUID().toString)
      // change eid to CB_AUDIT_ITEM
      newEventMap.put("eid", "CB_ITEM")
      // TODO: update ets and timestamp
      // create a shallow copy of eventMapEData excluding cb_data
      val newEventMapEData = mergedMap(List(MapMergeParams(eventMapEData, "", List("cb_data"))))
      // update new edata, put flatWorkOrderMap as cb_data
      newEventMapEData.put("cb_data", flatEventData.map)
      // update new event map, put newEventMapEData as edata
      newEventMap.put("edata", newEventMapEData)
      (new Event(newEventMap), flatEventData.childType, flatEventData.hasRole)
    })
  }

}
