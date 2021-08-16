// FRAC

let frac_event = {
  "actor": {
    "id": "59c3c2b7-b32b-4d9a-9d02-220e73004d66",  // MDO ADMIN id
    "type": "User"
  },
  "eid": "CB_AUDIT",
  "edata": {
    "state": "Draft",  // Draft/Published etc.
    "props": ["WAT/FRAC"],
    "cb_object": {
      "id": "9a99e795-c652-4c0d-9f9f-960c737e15f3",  // Competency/Activity/Role id
      "type": "Competency",  // type=Competency/Activity/Role
      "ver": "1.0", // 1.0 for now
      "name": "Coding",  // Competency/Activity/Role name
      "org": "ISTM",  // org, eg. ISTM
      "sub_type": "Functional"  // only present for competency, e.g Functional, empty string for others
    }
  },
  "ver": "3.0",
  "ets": 1628845042921,
  "context": {
    "channel": "013260789496258560586",  // must be present
    "pdata": {
      "id": "dev.FRac.portal",
      "pid": "FRac",
      "ver": "1.0"
    },
    "env": "WAT"
  },
  "mid": "CB.d0152fb1-cd84-4309-882a-88b1df0e1648",  // mid="CB.<uuid>"
  "object": {
    "id": "9a99e795-c652-4c0d-9f9f-960c737e15f3",  // Competency/Activity/Role id
    "type": "Competency"  // Competency/Activity/Role
  }
}

// WAT

let wat_event = {
  "actor": {
    "id": "59c3c2b7-b32b-4d9a-9d02-220e73004d66",  // User creating this event
    "type": "User"
  },
  "eid": "CB_AUDIT",
  "edata": {
    "state": "Draft",  // Draft/Published etc.
    "props": ["WAT"],
    "cb_object": {
      "id": "643cb47c-3e8a-4d5e-9fd4-45302a9ae09a",  // work order id
      "type": "WorkOrder",  // WorkOrder
      "ver": "1.0",
      "name": "Work order - Finance wing",
      "org": "New NHTest"
    },
    "cb_data": {}
  },
  "ver": "3.0",
  "ets": 1628845042921,
  "context": {
    "channel": "013260789496258560586", // must be present
    "pdata": {
      "id": "<env>.mdo.portal",
      "pid": "mdo",
      "ver": "1.0"
    },
    "env": "WAT"
  },
  "mid": "CB.b4b1a956-d8d5-48e4-8cee-0dc616823402",
  "object": {
    "id": "643cb47c-3e8a-4d5e-9fd4-45302a9ae09a",
    "type": "WorkOrder"
  }
}