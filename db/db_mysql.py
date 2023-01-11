import pymysql
import logging
import time

mysql_host = ""
mysql_database = ""
mysql_username = ""
mysql_password = ""


def write_mocount(oss, total, newer, old, older):
    logging.info('net_mysql; - connecting to mysql')
    try:
        logging.debug('host: %s, user: %s, pass: %s, db: %s' % (mysql_host, mysql_username, mysql_password, mysql_database))
        unixtime = int(time.time())
        con = pymysql.connect(mysql_host, mysql_username, mysql_password, mysql_database)
        insert_query = "INSERT IGNORE INTO sre.mocount (timestamp, ossid, total_mos, new_mos, old_mos, tbd_mos) " \
                       "VALUES (%s,%s,%s,%s,%s,%s)"

        logging.info('preparing insert')
        logging.info(insert_query % (unixtime, oss, total, newer, old, older))

        with con.cursor() as cur:
            cur.execute(insert_query, (unixtime, oss, total, newer, old, older))
            con.commit()

    except pymysql.Error as e:
        logging.ERROR('net_mysql - error inserting %d: %s' % (e.args[0], e.args[1]))
    finally:
        logging.info('net_mysql - finish inserting')
        con.close()
        logging.info('net_mysql - closing conection')


def write_roses(roses_info):
    logging.info('net_mysql; - connecting to mysql')
    try:
        logging.debug('host: %s, user: %s, pass: %s, db: %s' % (mysql_host, mysql_username, mysql_password, mysql_database))
        unixtime = int(time.time())
        con = pymysql.connect(mysql_host, mysql_username, mysql_password, mysql_database)
        insert_query = "INSERT IGNORE INTO sre.moroses (timestamp, projection_name, partition_key, roses) " \
                       "VALUES (%s,%s,%s,%s)"

        logging.info('net_mysql - inserting - %s -%s - %s - %s' % (unixtime, roses_info[0], roses_info[1], roses_info[2]))
        logging.info(insert_query % (unixtime, roses_info[0], roses_info[1], roses_info[2]))

        with con.cursor() as cur:
            cur.execute(insert_query, (unixtime, roses_info[0], roses_info[1], roses_info[2]))
            con.commit()

    except pymysql.Error as e:
        logging.ERROR('net_mysql - error inserting %d: %s' % (e.args[0], e.args[1]))
    finally:
        logging.info('net_mysql - finish inserting')
        con.close()
        logging.info('net_mysql - closing conection')


def write_token(token_info):
    logging.info('net_mysql; - connecting to mysql')
    try:
        logging.debug('host: %s, user: %s, pass: %s, db: %s' % (mysql_host, mysql_username, mysql_password, mysql_database))
        unixtime = int(time.time())
        con = pymysql.connect(mysql_host, mysql_username, mysql_password, mysql_database)
        insert_query = "INSERT IGNORE INTO sre.tokenmonitor (timestamp, env, status, resp_time ) " \
                       "VALUES (%s,%s,%s,%s)"

        logging.info('net_mysql - inserting - %s -%s - %s - %s' % (unixtime, token_info[0], token_info[1], token_info[2]))
        logging.info(insert_query % (unixtime, token_info[0], token_info[1], token_info[2]))

        with con.cursor() as cur:
            cur.execute(insert_query, (unixtime, token_info[0], token_info[1], token_info[2]))
            con.commit()

    except pymysql.Error as e:
        logging.ERROR('net_mysql - error inserting %d: %s' % (e.args[0], e.args[1]))
    finally:
        logging.info('net_mysql - finish inserting')
        con.close()
        logging.info('net_mysql - closing conection')


def write_oss_updates(oss, updated):
    logging.info('net_mysql; - connecting to mysql')
    try:
        logging.debug('host: %s, user: %s, pass: %s, db: %s' % (mysql_host, mysql_username, mysql_password, mysql_database))
        unixtime = int(time.time())
        con = pymysql.connect(mysql_host, mysql_username, mysql_password, mysql_database)
        insert_query = "INSERT IGNORE INTO sre.ossupdated (timestamp, ossid, updated) " \
                       "VALUES (%s,%s,%s)"

        logging.info(insert_query % (unixtime, oss, updated))

        with con.cursor() as cur:
            cur.execute(insert_query, (unixtime, oss, updated))
            con.commit()

    except pymysql.Error as e:
        logging.ERROR('net_mysql - error inserting %d: %s' % (e.args[0], e.args[1]))
    finally:
        logging.info('net_mysql - finish inserting')
        con.close()
        logging.info('net_mysql - closing conection')


def get_ossinfo():
    logging.info('net_mysql - connecting to mysql')
    try:
        logging.debug('net_mysql - host: %s, user: %s, pass: %s, db: %s' % (mysql_host, mysql_username, mysql_password,
                                                                mysql_database))

        con = pymysql.connect(mysql_host, mysql_username, mysql_password, mysql_database)

        with con.cursor() as cur:
            cur.execute('SELECT id,description, vendor FROM sre.ossinfo')
            rows = cur.fetchall()
            for row in rows:
                logging.debug(f'{row[0]} {row[1]} {row[2]}')

            return rows

    except pymysql.Error as e:
        logging.ERROR('net_mysql - error running %d: %s' % (e.args[0], e.args[1]))
    finally:
        logging.info('net_mysql - finish querying')
        con.close()
        logging.info('net_mysql - closing connection')
