import logging
import vertica_python

vertica_mo = ""
vertica_database = ""
vertica_user = ""
vertica_password = ""


lte_sectorclass = {
    'ERICSSON': 'vsDataEUtranCellFDD',
    'SAMSUNG': 'eutran-cell-fdd-tdd',
    'NOKIA': 'LNCELL'
}
nr_sectorclass = {
    'ERICSSON': 'vsDataNRCellDU',
    'NOKIA': 'NRCELL',
    'SAMSUNG': 'gutran-du-cell-entries'
}
lte_nodeclass = {
    'SAMSUNG': 'enb-function',
    'NOKIA': 'LNBTS',
    'ERICSSON': 'vsDataENodeBFunction'
}
nr_nodeclass = {
    'SAMSUNG': 'gnb-du-function',
    'NOKIA': 'NRBTS',
    'ERICSSON': 'vsDataGNBCUCPFunction'
}


def get_mocount(oss, vendor):
    conn_info = {'host': vertica_mo,
                 'port': 5433,
                 'user': vertica_user,
                 'password': vertica_password,
                 'database': vertica_database}

    logging.info('net_vertica - Connecting to vertica')
    
    vend = ""
    if vendor == 'ERICSSON':
        vend = 'e'
    if vendor == 'SAMSUNG':
        vend = 'sa'
    if vendor == 'NOKIA':
        vend = 'n'

    # Connect to a vertica database
    try:
        with vertica_python.connect(**conn_info) as conn:
            # Open a cursor to perform database operations
            cur = conn.cursor()

            # Execute a command: create a table
            logging.info('net_vertica - About to query oss %s' % (oss))

            query = "select count(*) from sonmo.managed_objects where \"META.OSSID\" = %s and (class = \'%s\' or class = \'%s\') and vendor = \'%s\'"

            logging.info(query % (str(oss), lte_sectorclass[vendor], nr_sectorclass[vendor], vend))

            cur.execute(query % (oss, lte_sectorclass[vendor], nr_sectorclass[vendor], vend))
            
            result = cur.fetchone()
            logging.info('net_vertica - Returning result %s ' % result)

        return result[0]
    except vertica_python.errors as e:
        logging.ERROR('net_vertica - error inserting %d: %s' % (e.args[0], e.args[1]))
    finally:
        logging.info('net_vertica - finish querying')
        conn.close()
        logging.info('net_vertica - closing connection')


def get_oss_update_today(oss, vendor):
    conn_info = {'host': vertica_mo,
                 'port': 5433,
                 'user': vertica_user,
                 'password': vertica_password,
                 'database': vertica_database}

    logging.info('net_vertica - Connecting to vertica')
    vend = ""
    if vendor == 'ERICSSON':
        vend = 'e'
    if vendor == 'SAMSUNG':
        vend = 'sa'
    if vendor == 'NOKIA':
        vend = 'n'

    # Connect to a vertica database
    try:
        with vertica_python.connect(**conn_info) as conn:
            # Open a cursor to perform database operations
            cur = conn.cursor()

            # Execute a command: create a table
            query = "select count(*) from sonmo.managed_objects " \
                    "where \"META.OSSID\" = %s  and (class = \'%s\' or class =\'%s\') and vendor = \'%s\' and " \
                    "DATE(TO_TIMESTAMP(\"META.UPDATED_AT\"/1000)) = DATE((CURRENT_TIMESTAMP - INTERVAL '4 HOUR'))"

            logging.info(query % (str(oss), lte_sectorclass[vendor],nr_sectorclass[vendor], vendor))

            cur.execute(query % (oss, lte_sectorclass[vendor], nr_sectorclass[vendor], vend ))

            result = cur.fetchone()
            logging.info('net_vertica - Returning result %s ' % result)

        return result[0]
    except vertica_python.errors as e:
        logging.ERROR('net_vertica - error querying %d: %s' % (e.args[0], e.args[1]))
    finally:
        logging.info('net_vertica - finish querying')
        conn.close()
        logging.info('net_vertica - closing connection')


def get_oss_update_count(oss, interval, vendor):
    conn_info = {'host': vertica_mo,
                 'port': 5433,
                 'user': vertica_user,
                 'password': vertica_password,
                 'database': vertica_database}

    logging.info('net_vertica - Connecting to vertica')
    vend = ""
    if vendor == 'ERICSSON':
        vend = 'e'
    if vendor == 'SAMSUNG':
        vend = 'sa'
    if vendor == 'NOKIA':
        vend = 'n'

    # Connect to a vertica database
    try:
        with vertica_python.connect(**conn_info) as conn:
            # Open a cursor to perform database operations
            cur = conn.cursor()

            # Execute a command: create a table
            query = "select count(*) from sonmo.managed_objects " \
                    "where \"META.OSSID\" = %s  and (class = \'%s\' or class =\'%s\') and vendor = \'%s\' and " \
                    "TO_TIMESTAMP(\"META.UPDATED_AT\"/1000) > (CURRENT_TIMESTAMP - INTERVAL '%s')"

            logging.info(query % (str(oss), lte_sectorclass[vendor],nr_sectorclass[vendor], vend, str(interval)))

            cur.execute(query % (oss, lte_sectorclass[vendor], nr_sectorclass[vendor], vend, interval))

            result = cur.fetchone()
            logging.info('net_vertica - Returning result %s ' % result)

        return result[0]
    except vertica_python.errors as e:
        logging.ERROR('net_vertica - error querying %d: %s' % (e.args[0], e.args[1]))
    finally:
        logging.info('net_vertica - finish querying')
        conn.close()
        logging.info('net_vertica - closing connection')


def get_oss_outdated_count(oss, interval, vendor):
    conn_info = {'host': vertica_mo,
                 'port': 5433,
                 'user': vertica_user,
                 'password': vertica_password,
                 'database': vertica_database}

    logging.info('net_vertica - Connecting to vertica')
    vend = ""
    if vendor == 'ERICSSON':
        vend = 'e'
    if vendor == 'SAMSUNG':
        vend = 'sa'
    if vendor == 'NOKIA':
        vend = 'n'

    # Connect to a vertica database
    try:
        with vertica_python.connect(**conn_info) as conn:
            # Open a cursor to perform database operations
            cur = conn.cursor()

            # Execute a command: create a table
            query = "select count(*) from sonmo.managed_objects " \
                    "where \"META.OSSID\" = %s and (class = \'%s\' or class =\'%s\') and vendor = \'%s\' and " \
                    "TO_TIMESTAMP(\"META.UPDATED_AT\"/1000) < (CURRENT_TIMESTAMP - INTERVAL '%s')"

            logging.info(query % (str(oss), lte_sectorclass[vendor], nr_sectorclass[vendor], vend, str(interval)))

            cur.execute(query % (oss, lte_sectorclass[vendor], nr_sectorclass[vendor], vend, interval))

            result = cur.fetchone()
            logging.info('net_vertica - Returning result %s ' % result)

        return result[0]
    except vertica_python.errors as e:
        logging.ERROR('net_vertica - error querying %d: %s' % (e.args[0], e.args[1]))
    finally:
        logging.info('net_vertica - finish querying')
        conn.close()
        logging.info('net_vertica - closing connection')

def get_roses_count():
    conn_info = {'host': vertica_mo,
                 'port': 5433,
                 'user': vertica_user,
                 'password': vertica_password,
                 'database': vertica_database}

    logging.info('net_vertica - Connecting to vertica')

    # Connect to a vertica database
    try:
        with vertica_python.connect(**conn_info) as conn:
            # Open a cursor to perform database operations
            cur = conn.cursor()

            # Execute a command: create a table
            logging.info('net_vertica - About to query roses')
            cur.execute("select projection_name, partition_key, count(distinct ros_id) as roses FROM PARTITIONS "
                        "where REGEXP_LIKE(projection_name,'managed_objects_source') group by 1,2")
            result = cur.fetchall()
            logging.info('net_vertica - Returning result %s ' % result)

        return result
    except vertica_python.errors as e:
        logging.ERROR('net_vertica - error querying %d: %s' % (e.args[0], e.args[1]))
    finally:
        logging.info('net_vertica - finish querying')
        conn.close()
        logging.info('net_vertica - closing connection')


def find_mo_exist(oss, uid, vendor, moclass):
    conn_info = {'host': vertica_mo,
                 'port': 5433,
                 'user': vertica_user,
                 'password': vertica_password,
                 'database': vertica_database}

    #logging.info('net_vertica - Connecting to vertica')
    vend = ""
    if vendor == 'ERICSSON':
        vend = 'e'
    if vendor == 'SAMSUNG':
        vend = 'sa'
    if vendor == 'NOKIA':
        vend = 'n'


    # Connect to a vertica database
    try:
        with vertica_python.connect(**conn_info) as conn:
            # Open a cursor to perform database operations
            cur = conn.cursor()

            # Execute a command: create a table
            query = "select to_char(uid) from sonmo.managed_objects " \
                    "where \"META.OSSID\" = %s  and class = \'%s\' and vendor = \'%s\' and uid = \'%s\'" 

            #logging.info(query % (str(oss), moclass, vend, uid))
            cur.execute(query % (str(oss), moclass, vend, uid))

            result = cur.fetchone()
            if not result:
              return False
            else:
              return True

        return result[0]
    except vertica_python.errors as e:
        logging.ERROR('net_vertica - error querying %d: %s' % (e.args[0], e.args[1]))
    finally:
        #logging.info('net_vertica - finish querying')
        conn.close()
        #logging.info('net_vertica - closing connection')


def find_mo_bulk(oss, uid, vendor, moclass, qsize):
    conn_info = {'host': vertica_mo,
                 'port': 5433,
                 'user': vertica_user,
                 'password': vertica_password,
                 'database': vertica_database}

    #logging.info('net_vertica - Connecting to vertica')
    vend = ""
    if vendor == 'ERICSSON':
        vend = 'e'
    if vendor == 'SAMSUNG':
        vend = 'sa'
    if vendor == 'NOKIA':
        vend = 'n'


    # Connect to a vertica database
    try:
        with vertica_python.connect(**conn_info) as conn:
            # Open a cursor to perform database operations
            cur = conn.cursor()

            # Execute a command: create a table
            query = "select count(uid) from sonmo.managed_objects " \
                    "where \"META.OSSID\" = %s  and class = \'%s\' and vendor = \'%s\' and uid in (%s)" 

            #logging.info(query % (str(oss), moclass, vend, uid))
            cur.execute(query % (str(oss), moclass, vend, uid))

            result = cur.fetchone()

            if not result:
              return False
            elif qsize != int(result[0]):
              logging.info('error expectin: %s got %s' % ( qsize, result[0]))
              return False
            else:
              return True

        return result[0]
    except vertica_python.errors as e:
        logging.ERROR('net_vertica - error querying %d: %s' % (e.args[0], e.args[1]))
    finally:
        #logging.info('net_vertica - finish querying')
        conn.close()
        #logging.info('net_vertica - closing connection')

