import logging
import sys

from pymongo import MongoClient, errors

import conf.confTest
from main import print_hi

mongo_port = "27017"
mongo_sector_collection = "BaseSector"
mongo_db = conf.confTest.mongo_host
mongo_user = "triangulum12"
mongo_password = "sonofagun34"
mongo_host = "192.168.93.24"
mongo_database = "admin"
mongo_database_network = "bell_network"


def open_connection():
    logging.info('connecting to mongo')
    return MongoClient(mongo_host,
                       username=mongo_user,
                       password=mongo_password,
                       authSource="admin",
                       authMechanism='SCRAM-SHA-1')


def close_connection(connection):
    connection.close()


def get_data_by_query(connection, query, projection=None):
    try:
        # select database
        database = connection[mongo_database_network]

        # select collection
        collection = database[mongo_sector_collection]

        logging.info('Querying -- filter: %s | project: %s' % (query, projection))

        result = collection.find(query, projection)

        resultMap = {}
        for doc in result:
            resultMap[str(doc['_id'])] = doc

        return resultMap

    except errors.PyMongoError as py_mongo_error:
        logging.info('Error in MongoDB connection: ' + str(py_mongo_error))


def get_aggregation_by_query(connection, query):
    try:
        # select database
        database = connection[mongo_database_network]

        # select collection
        collection = database[mongo_sector_collection]

        logging.info('Querying -- filter: %s' % (query))

        result = collection.aggregate(query)

        resultMap = {}
        for doc in result:
            resultMap[str(doc['_id'])] = doc

        w = []
        query_results = dict()
        for key in resultMap.keys():
            val = resultMap[key]
            w.append(val)
            query_results[str(val['_id'])] = str(val['count'])

        # print(m)

        return query_results



    except errors.PyMongoError as py_mongo_error:
        logging.info('Error in MongoDB connection: ' + str(py_mongo_error))


def get_sector_uid_list(oss, type):

    resultMap = {}
    try:
        connection = open_connection()
        # MongoClient(mongo_host,
        #                          username=mongo_user,
        #                          password=mongo_password,
        #                          authSource="admin",
        #                          authMechanism='SCRAM-SHA-1')
        #

        # select database
        database = connection[mongo_database_network]

        # select collection
        collection = database[mongo_sector_collection]

        query = {"ossId": oss, "_type": type,
                 "external": False,
                 "stub": False,
                 "lteMode": {"$nin": ['NBIOT_FDD', 'TDD']}
                 }

        projection = {'_id': 1}
        # 'name': 1, 'guid': 1, 'updatedAt': 1, 'lteMode': 1}

        logging.info('Querying -- filter: %s | project: %s' % (query, projection))

        result = collection.find(query, projection)
        result1 = collection.find(query)

        # for x in database[mongo_sector_collection].find({"ossId": "4"}):
        #     print(x)

        for doc in result:
            resultMap[str(doc['_id'])] = doc

        # print(sys.getsizeof(dict), "Size of dict")
        # print(sys.getsizeof(doc), "Size of doc")
        # print(sys.getsizeof(result), "Size of result")

        return resultMap

    except errors.PyMongoError as py_mongo_error:
        logging.info('Error in MongoDB connection: ' + str(py_mongo_error))
    finally:
        close_connection(connection)


def get_mongo_data(database, collection, query, projection):
    logging.info('connecting to mongo')
    resultset = {}
    try:

        collection = database[collection]

        logging.info('Querying -- filter: %s | project: %s' % (query, projection))
        result = collection.find(query, projection)

        for doc in result:
            resultset[str(doc['_id'])] = doc

        return resultset

    except errors.PyMongoError as py_mongo_error:
        logging.info('Error in MongoDB connection: ' + str(py_mongo_error))
