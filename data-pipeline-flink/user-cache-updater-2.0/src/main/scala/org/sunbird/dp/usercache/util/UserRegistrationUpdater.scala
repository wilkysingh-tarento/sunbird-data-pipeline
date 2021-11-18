package org.sunbird.dp.usercache.util

import com.google.gson.Gson
import org.slf4j.LoggerFactory
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.DataCache
import org.sunbird.dp.core.job.Metrics
import org.sunbird.dp.usercache.domain.Event
import org.sunbird.dp.usercache.task.UserCacheUpdaterConfigV2

import scala.collection.JavaConverters._
import scala.collection.mutable

case class UserReadResult(result: java.util.HashMap[String, Any], responseCode: String, params: Params)
case class Response(firstName: String, lastName: String, encEmail: String, encPhone: String, language: java.util.List[String], rootOrgId: String, profileUserType: java.util.HashMap[String, String],
                    userLocations: java.util.ArrayList[java.util.Map[String, AnyRef]], rootOrg: RootOrgInfo, userId: String, framework: java.util.LinkedHashMap[String, java.util.List[String]])
case class RootOrgInfo(orgName: String)
case class Params(msgid: String, err: String, status: String, errmsg: String)

object UserRegistrationUpdater {

  private lazy val gson = new Gson()

  val logger = LoggerFactory.getLogger("UserRegistrationUpdater")

  def execute(userData :mutable.Map[String, AnyRef], RootOrgName : String, dataCache: DataCache): mutable.Map[String, AnyRef] = {
    val OrgName = RootOrgName
    val count =  1
    dataCache.hinCr(OrgName)
    userData+=("incCount" -> count+1.asInstanceOf[String])


  }


}
