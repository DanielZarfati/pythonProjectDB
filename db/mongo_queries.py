def get_oss_and_sector(oss, _type):
    return {"ossId": oss, "_type": _type,
            "external": False,
            "stub": False,
            "lteMode": {"$nin": ['NBIOT_FDD', 'TDD']}
            }


def get_oss_query(oss):
    return {"ossId": oss,
            "external": False,
            "stub": False,
            "lteMode": {"$nin": ['NBIOT_FDD', 'TDD']}
            }


def get_aggregated_query():
    return [{"$match": {"stub": False, "external": False,
                        "physicalIdentifier": {"$ne": None}}
             },
            {"$group": {"_id": {"vendor": "$vendor", "mcc": "$mcc", "mnc": "$mnc", "lteMode": "$lteMode"},
                        "count": {"$sum": 1}}
             }, {
                "$sort": {"vendor": 1, "mnc": 1, "_type": 1}}]


def get_aggregated_type_vendor_query():
    return [{"$group": {"_id": {"vendor": "$vendor", "type": "$_type"}
        , "count": {"$sum": 1}}}, {"$sort": {"vendor": 1, "type": 1}}]


def get_aggregated_type_vendor_stub_external_false_query():
    return ([{"$match": {"stub": False, "external": False}}, {"$group": {"_id": {"vendor": "$vendor", "type": "$_type"}
        , "count": {"$sum": 1}}}, {"$sort": {"vendor": 1, "type": 1}}])


def get_aggregated_type_vendor_stub_external_false_mnc_empty_query():
    return ([{"$match": {"stub": False, "external": False, "mnc": None}}, {"$group": {"_id":
                                                                                          {"vendor": "$vendor",
                                                                                           "type": "$_type"}
        , "count": {"$sum": 1}}}, {"$sort": {"vendor": 1, "type": 1}}])


def get_aggregated_nbrtype_sectorlevelneighbor_query():
    return ([{"$group": {"_id": {"nbr_type": "$nbr_type", "type": "$_type"}
        , "count": {"$sum": 1}}}, {"$sort": {"nbr_type": 1, "type": 1}}])

    # Projection


def get_projection(field):
    return {"'" + field + "'": 1}


def get_projection1(field, field2):
    return {"'" + field + "'": 1, field2: 1}
