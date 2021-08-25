package org.sunbird.dp.fixture

object CBEventFixture {

  val SAMPLE_WO_EVENT: String =
    """{
      |  "actor": {
      |    "id": "59c3c2b7-b32b-4d9a-9d02-220e73004d66",
      |    "type": "User"
      |  },
      |  "eid": "CB_AUDIT",
      |  "edata": {
      |    "state": "Published",
      |    "props": ["WAT"],
      |    "cb_object": {
      |      "id": "643cb47c-3e8a-4d5e-9fd4-45302a9ae09a",
      |      "type": "WorkOrder",
      |      "ver": "1.0",
      |      "name": "Work order - Finance wing",
      |      "org": "New NHTest"
      |    },
      |    "cb_data": {
      |      "id": "643cb47c-3e8a-4d5e-9fd4-45302a9ae09a",
      |      "name": "Work order - Finance wing",
      |      "deptId": "013260789496258560586",
      |      "deptName": "New NHTest",
      |      "status": "Draft",
      |      "userIds": [
      |        "3f90ed64-2cba-4e14-8844-1ec53da454f8"
      |      ],
      |      "createdBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |      "createdAt": 1628844512397,
      |      "updatedBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |      "updatedAt": 1628845042921,
      |      "progress": 91,
      |      "errorCount": 0,
      |      "rolesCount": 1,
      |      "activitiesCount": 1,
      |      "competenciesCount": 1,
      |      "publishedPdfLink": null,
      |      "signedPdfLink": null,
      |      "mdo_name": "New NHTest",
      |      "users": [
      |        {
      |          "id": "3f90ed64-2cba-4e14-8844-1ec53da454f8",
      |          "userId": "535c8d83-e5ed-4b91-82eb-89031702dcc9",
      |          "roleCompetencyList": [
      |            {
      |              "roleDetails": {
      |                "type": "ROLE",
      |                "id": "id01",
      |                "name": "Management role",
      |                "description": "",
      |                "status": null,
      |                "source": null,
      |                "childNodes": [
      |                  {
      |                    "type": "ACTIVITY",
      |                    "id": "id01",
      |                    "name": "",
      |                    "description": "Manager role",
      |                    "status": null,
      |                    "source": null,
      |                    "parentRole": null,
      |                    "submittedFromId": null,
      |                    "submittedToId": "",
      |                    "level": null
      |                  }
      |                ],
      |                "addedAt": 0,
      |                "updatedAt": 0,
      |                "updatedBy": null,
      |                "archivedAt": 0,
      |                "archived": false
      |              },
      |              "competencyDetails": [
      |                {
      |                  "type": "COMPETENCY",
      |                  "id": "id01",
      |                  "name": "behavioural competency profiling",
      |                  "description": "behavioural competency profiling desc",
      |                  "source": null,
      |                  "status": null,
      |                  "level": "Level 1",
      |                  "additionalProperties": {
      |                    "competencyArea": "Area",
      |                    "competencyType": "Behavioural"
      |                  },
      |                  "children": null
      |                }
      |              ]
      |            }
      |          ],
      |          "unmappedActivities": [],
      |          "unmappedCompetencies": [],
      |          "userPosition": "Team management",
      |          "positionId": "id01",
      |          "positionDescription": "manage-teams",
      |          "workOrderId": "9a99e795-c652-4c0d-9f9f-960c737e15f3",
      |          "updatedAt": 1628845041770,
      |          "updatedBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |          "errorCount": 0,
      |          "progress": 91,
      |          "createdAt": 1628845041770,
      |          "createdBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c"
      |        }
      |      ]
      |    }
      |  },
      |  "ver": "3.0",
      |  "ets": 1629109359638,
      |  "context": {
      |    "channel": "013260789496258560586",
      |    "pdata": {
      |      "id": "dev.mdo.portal",
      |      "pid": "mdo",
      |      "ver": "1.0"
      |    },
      |    "env": "WAT"
      |  },
      |  "mid": "CB.b4b1a956-d8d5-48e4-8cee-0dc616823402",
      |  "object": {
      |    "id": "643cb47c-3e8a-4d5e-9fd4-45302a9ae09a",
      |    "type": "WorkOrder"
      |  }
      |}""".stripMargin

  val SAMPLE_WO_EVENT_FLATTENED: String =
    """[
      |    {
      |        "id": "643cb47c-3e8a-4d5e-9fd4-45302a9ae09a",
      |        "name": "Work order - Finance wing",
      |        "deptId": "013260789496258560586",
      |        "deptName": "New NHTest",
      |        "status": "Draft",
      |        "userIds": [
      |            "3f90ed64-2cba-4e14-8844-1ec53da454f8"
      |        ],
      |        "createdBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |        "createdAt": 1628844512397,
      |        "updatedBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |        "updatedAt": 1628845042921,
      |        "progress": 91,
      |        "errorCount": 0,
      |        "rolesCount": 1,
      |        "activitiesCount": 1,
      |        "competenciesCount": 1,
      |        "publishedPdfLink": null,
      |        "signedPdfLink": null,
      |        "mdo_name": "New NHTest",
      |        "users_id": "3f90ed64-2cba-4e14-8844-1ec53da454f8",
      |        "users_userId": "535c8d83-e5ed-4b91-82eb-89031702dcc9",
      |        "users_unmappedActivities": [],
      |        "users_unmappedCompetencies": [],
      |        "users_userPosition": "Team management",
      |        "users_positionId": "id01",
      |        "users_positionDescription": "manage-teams",
      |        "users_workOrderId": "9a99e795-c652-4c0d-9f9f-960c737e15f3",
      |        "users_updatedAt": 1628845041770,
      |        "users_updatedBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |        "users_errorCount": 0,
      |        "users_progress": 91,
      |        "users_createdAt": 1628845041770,
      |        "users_createdBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |        "users_roleCompetencyList_competencyDetails_type": "COMPETENCY",
      |        "users_roleCompetencyList_competencyDetails_id": "id01",
      |        "users_roleCompetencyList_competencyDetails_name": "behavioural competency profiling",
      |        "users_roleCompetencyList_competencyDetails_description": "behavioural competency profiling desc",
      |        "users_roleCompetencyList_competencyDetails_source": null,
      |        "users_roleCompetencyList_competencyDetails_status": null,
      |        "users_roleCompetencyList_competencyDetails_level": "Level 1",
      |        "users_roleCompetencyList_competencyDetails_additionalProperties": {
      |            "competencyArea": "Area",
      |            "competencyType": "Behavioural"
      |        },
      |        "users_roleCompetencyList_competencyDetails_children": null
      |    },
      |    {
      |        "id": "643cb47c-3e8a-4d5e-9fd4-45302a9ae09a",
      |        "name": "Work order - Finance wing",
      |        "deptId": "013260789496258560586",
      |        "deptName": "New NHTest",
      |        "status": "Draft",
      |        "userIds": [
      |            "3f90ed64-2cba-4e14-8844-1ec53da454f8"
      |        ],
      |        "createdBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |        "createdAt": 1628844512397,
      |        "updatedBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |        "updatedAt": 1628845042921,
      |        "progress": 91,
      |        "errorCount": 0,
      |        "rolesCount": 1,
      |        "activitiesCount": 1,
      |        "competenciesCount": 1,
      |        "publishedPdfLink": null,
      |        "signedPdfLink": null,
      |        "mdo_name": "New NHTest",
      |        "users_id": "3f90ed64-2cba-4e14-8844-1ec53da454f8",
      |        "users_userId": "535c8d83-e5ed-4b91-82eb-89031702dcc9",
      |        "users_unmappedActivities": [],
      |        "users_unmappedCompetencies": [],
      |        "users_userPosition": "Team management",
      |        "users_positionId": "id01",
      |        "users_positionDescription": "manage-teams",
      |        "users_workOrderId": "9a99e795-c652-4c0d-9f9f-960c737e15f3",
      |        "users_updatedAt": 1628845041770,
      |        "users_updatedBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |        "users_errorCount": 0,
      |        "users_progress": 91,
      |        "users_createdAt": 1628845041770,
      |        "users_createdBy": "075e3a3f-1a56-4ea3-9042-c66e2288e60c",
      |        "users_roleCompetencyList_roleDetails_type": "ROLE",
      |        "users_roleCompetencyList_roleDetails_id": "id01",
      |        "users_roleCompetencyList_roleDetails_name": "Management role",
      |        "users_roleCompetencyList_roleDetails_description": "",
      |        "users_roleCompetencyList_roleDetails_status": null,
      |        "users_roleCompetencyList_roleDetails_source": null,
      |        "users_roleCompetencyList_roleDetails_addedAt": 0,
      |        "users_roleCompetencyList_roleDetails_updatedAt": 0,
      |        "users_roleCompetencyList_roleDetails_updatedBy": null,
      |        "users_roleCompetencyList_roleDetails_archivedAt": 0,
      |        "users_roleCompetencyList_roleDetails_archived": false,
      |        "users_roleCompetencyList_roleDetails_childNodes_type": "ACTIVITY",
      |        "users_roleCompetencyList_roleDetails_childNodes_id": "id01",
      |        "users_roleCompetencyList_roleDetails_childNodes_name": "",
      |        "users_roleCompetencyList_roleDetails_childNodes_description": "Manager role",
      |        "users_roleCompetencyList_roleDetails_childNodes_status": null,
      |        "users_roleCompetencyList_roleDetails_childNodes_source": null,
      |        "users_roleCompetencyList_roleDetails_childNodes_parentRole": null,
      |        "users_roleCompetencyList_roleDetails_childNodes_submittedFromId": null,
      |        "users_roleCompetencyList_roleDetails_childNodes_submittedToId": "",
      |        "users_roleCompetencyList_roleDetails_childNodes_level": null
      |    }
      |]""".stripMargin

}
