import db.db_mongo as mongo_client
import db.mongo_queries as m_query

def mongo_by_vendor(query_results):
    vendor_list = ['ERICSSON', 'NOKIA', 'SAMSUNG', 'HUAWEI']
    tech_list = ['UtranSector', 'NrSector', 'LTESector', 'GsmSector']
    dict_results = {}
    for i in query_results:
        for vendor in vendor_list:
            if vendor in i:
                crr_vendor = vendor
                if vendor not in dict_results:
                    dict_results[vendor] = {}
        for tech in tech_list:
            if tech in i and tech not in dict_results[crr_vendor]:
                dict_results[crr_vendor][tech]=[query_results[i]]
    print(dict_results)


def mongo_by_nbrtype(query_results):
    nbr_type = ['IRAT_LTE_NR', 'IRAT_LTE_UMTS', 'LTE_INTER', 'LTE_INTRA','SHO','NR_INTRA','NR_INTER','IRAT_NR_LTE','IFHO']
    type_list = ['IRATNeighborLTENR', 'IRATNeighborLTEUMTS', 'LteSectorLevelNeighbor', 'LteSectorLevelNeighbor','UtranNeighbor','NrNeighborNrNr','NrNeighborNrNr','IRATNeighborNRLTE','UtranNeighbor']
    dict_results = {}
    for i in query_results:
        for vendor in nbr_type:
            if vendor in i:
                crr_vendor = vendor
                if vendor not in dict_results:
                    dict_results[vendor] = {}
        for tech in type_list:
            if tech in i and tech not in dict_results[crr_vendor]:
                dict_results[crr_vendor][tech]=[query_results[i]]
    print(dict_results)

def test_mongo():
    connection = mongo_client.open_connection()

    # query = m_query.get_oss_and_sector("4", "NrSector")
    # query1 = m_query.get_oss_query("1")
    query2 = m_query.get_aggregated_query()
    query3 = m_query.get_aggregated_type_vendor_query()
    query4 = m_query.get_aggregated_type_vendor_stub_external_false_query()
    query5 = m_query.get_aggregated_type_vendor_stub_external_false_mnc_empty_query()
    query6 = m_query.get_aggregated_nbrtype_sectorlevelneighbor_query()

    projection = m_query.get_projection("_id")
    projection1 = m_query.get_projection1("_id", "_type")

    # result = mongo_client.get_data_by_query(connection, query, projection)
    # result1 = mongo_client.get_data_by_query(connection, query1, projection1)
    result2 = mongo_client.get_aggregation_by_query(connection, query2)
    result3 = mongo_client.get_aggregation_by_query(connection,query3)
    result4 = mongo_client.get_aggregation_by_query(connection,query4)
    result5 = mongo_client.get_aggregation_by_query(connection,query5)
    result6 = mongo_client.get_aggregation_by_query(connection,query6)


    mongo_by_vendor(result2)
    mongo_by_vendor(result3)
    mongo_by_vendor(result4)
    mongo_by_vendor(result5) #Should be empty
    print(result6)
    mongo_by_nbrtype(result6)


    # TODO here - sql db

    mongo_client.close_connection(connection)
