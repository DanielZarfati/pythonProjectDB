import argparse
import csv
import datetime
from conf import configuration
from db import db_mongo
from db import db_vertica
from db import db_mysql
import logging

result = {}

lte_sectorclass = {
    'ERICSSON': 'vsDataEUtranCellFDD',
    'SAMSUNG': 'eutran-cell-fdd-tdd',
    'NOKIA': 'LNCEL'
}
nr_sectorclass = {
    'ERICSSON': 'vsDataNRCellDU',
    'NOKIA': 'NRCELL',
    'SAMSUNG': 'gutran-du-cell-entries'
}
  

def check_bad_batch(evakeys, ossid, vendor, moclass):
  # check if the number of elements match if not return false
  global result
  #Define file location of the output
  filename = "/opt/scripts/Git/sre_dml_tools/abstract_mo/result/%s_%s_%s.csv" % (vendor, ossid, moclass)
  
  #Open existing file to add error information
  with open(str(filename), mode='a+') as resultfile:
    result_writer = csv.writer(resultfile, delimiter=',', quotechar='"',  quoting=csv.QUOTE_MINIMAL)
    # Iterating bad batch
    for uid in evakeys:
      if not db_vertica.find_mo_exist(ossid, uid, vendor, moclass): 
        #Getting info object data
        #uid = result[uid]['_id']
        name = result[uid]['name']
        guid = result[uid]['guid']
        odate = result[uid]['updatedAt']
        convdate = datetime.datetime.fromtimestamp(odate/1000.0)
       
        logging.info('!!! Orphan Object UID: %s, GUID: %s, NAME: %s, UPDATED: %s | %s' % (uid, guid, name, odate, convdate)) 
        
        #Writting and flushing
        result_writer.writerow([uid,guid,name,odate,convdate])
        resultfile.flush()


def check_oss(ossid, vendor):
  global result
  #Get Info from mongo
  logging.info('querying mongo for LTE info')
  result = db_mongo.get_sector_uid_list(str(ossid), 'LTESector')
  moclass = lte_sectorclass[vendor]
  logging.info("class %s - # of results %s" % (moclass,len(result)))
  
  chunksize = 250;
  iteration = 0
  total_iteration = 0
  resultkeys = result.keys()
  resultsize = len(result)
  evakeys = []
  uidlist = "" 
 
  #Iterate result in chunks
  for key in resultkeys:
    iteration += 1
    total_iteration +=1

    if iteration == chunksize or total_iteration == resultsize:
      logging.info('iter: %s pending: %s' % (iteration, resultsize - total_iteration))
      uid = result[key]['_id']
      evakeys.append(uid)
      uidlist = uidlist + "\'" + str(uid) + "\'"
      #Query vertica for uid list
      if not db_vertica.find_mo_bulk(ossid, uidlist, vendor, moclass,len(evakeys)):
        logging.info('bad batch')
        check_bad_batch(evakeys, ossid, vendor, moclass)
        logging.info('objects pending to check : %s' % (resultsize))

      #cleaning for next iteration
      iteration = 0
      evakeys = []
      uidlist = ""
    else:
      uid = result[key]['_id']
      evakeys.append(uid)
      uidlist = uidlist + "\'" + str(uid) + "\',"


    
  logging.info('querying mongo for NR info')
  result = db_mongo.get_sector_uid_list(str(ossid), 'NrSector')
  moclass = nr_sectorclass[vendor]
  resultsize = len(result)
  logging.info("class %s - # of results %s" % (moclass,len(result)))

  #iteration
  chunksize = 250;
  iteration = 0
  total_iteration = 0
  resultkeys = result.keys()
  resultsize = len(result)
  evakeys = []
  uidlist = "" 
 
  #Iterate result in chunks
  for key in resultkeys:
    iteration += 1
    total_iteration +=1

    if iteration == chunksize or total_iteration == resultsize:
      logging.info('iter: %s pending: %s' % (iteration, resultsize - total_iteration))
      uid = result[key]['_id']
      evakeys.append(uid)
      uidlist = uidlist + "\'" + str(uid) + "\'"
      #Query vertica for uid list
      if not db_vertica.find_mo_bulk(ossid, uidlist, vendor, moclass,len(evakeys)):
        logging.info('bad batch')
        check_bad_batch(evakeys, ossid, vendor, moclass)
        logging.info('objects pending to check : %s' % (resultsize))

      #cleaning for next iteration
      iteration = 0
      evakeys = []
      uidlist = ""
    else:
      uid = result[key]['_id']
      evakeys.append(uid)
      uidlist = uidlist + "\'" + str(uid) + "\',"
 
  logging.info('finish %s - %s' % (vendor, ossid))
  #input("Press Enter to continue...")

if __name__ == '__main__':
  logging.basicConfig(
      format='%(asctime)s %(levelname)-8s %(message)s',
      level=logging.INFO,
      datefmt='%Y-%m-%d %H:%M:%S')

  logging.info('main - Starting')
  parser = argparse.ArgumentParser(description='Fetch info - conf input ')
  parser.add_argument("conffinput", help="File with configuration")
  args = parser.parse_args()
  conf_file = str(args.conffinput)

  configuration.param_initialize(conf_file)

  logging.info('settig mysql up')
  db_mysql.mysql_host = str(configuration.conn_values['mysql_host'])
  db_mysql.mysql_database = str(configuration.conn_values['mysql_database'])
  db_mysql.mysql_username = str(configuration.conn_values['mysql_user'])
  db_mysql.mysql_password = str(configuration.conn_values['mysql_password'])

  logging.info('setting mongo up')
  db_mongo.mongo_user = str(configuration.conn_values['mongo_user'])
  db_mongo.mongo_password = str(configuration.conn_values['mongo_password'])
  db_mongo.mongo_host = str(configuration.conn_values['mongo_host'])
  db_mongo.mongo_database = str(configuration.conn_values['mongo_database'])
  db_mongo.mongo_database_network = str(configuration.conn_values['mongo_database_network'])

  logging.info('setting vertica')
  db_vertica.vertica_mo = str(configuration.conn_values['vertica_mo'])
  db_vertica.vertica_database = str(configuration.conn_values['vertica_database'])
  db_vertica.vertica_user = str(configuration.conn_values['vertica_user'])
  db_vertica.vertica_password = str(configuration.conn_values['vertica_password']) 

  oss_list = db_mysql.get_ossinfo()

  for oss in oss_list:
    logging.info('main - Querying vertica for %s - %s - %s ' % (oss[0],oss[1], oss[2]))    
    check_oss(oss[0], oss[2])

  
  logging.info('finish')
