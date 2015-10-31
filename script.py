from Queue import Queue
from threading import Thread
from time import time
import MySQLdb
import logging,sys
from utils import get_contact_info

TABLE = "googleimagefound"
DB_HOST = "localhost"
DB_NAME = "webinfo"
USER = "root"
PASSWORD = ""
COUNTRIES_LIST = ["DE","AT","CH"]
NUM_THREADS = 8

db = MySQLdb.connect(host = DB_HOST,db = DB_NAME,user = USER,passwd = PASSWORD)


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('requests').setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)

successfull = 0

class GetContactInfo(Thread):
	def __init__(self,threadID,queue,cursor):
		Thread.__init__(self)
		self.threadID = threadID
		self.queue = queue
		self.cursor = cursor


	def run(self):
		global successfull
		while True:
			url,sql_id = self.queue.get()
			impressum = get_contact_info(url)
			if impressum is not None:
				# update_db(self.cursor,impressum,sql_id)
				logger.info("Updated sql id:%s URL: %s ===> Impressum: %s",sql_id,url,impressum)
				if impressum != "http://www.bildplagiat.de":
					successfull += 1
			else:
				logger.info("Failed to update impressum for id: %s",sql_id)
			self.queue.task_done()



def get_urls(table,flag=0):
	sql = 'select page_url,id from %s where imprint=\"\" '%(table) #add semicolon
	if flag:
		sql=sql[0:-1] + " and ( "
		for country in COUNTRIES_LIST:
			if country != COUNTRIES_LIST[-1]:
				sql+= 'IP = \"%s\" OR '%(country)
			else:
				sql+= 'IP = \"%s\" )'%(country) #add the semicolon
	sql += " limit 20;"
	cur = db.cursor()
	cur.execute(sql)
	data = cur.fetchall()
	cur.close()
	return data
 
def update_db(cursor,impressum,sql_id):
	cur = cursor
	while True:
		try:
			cur.execute(update_table(impressum,sql_id))
		except MySQLdb.OperationalError:
			db = MySQLdb.connect(host = DB_HOST,db = DB_NAME,user = USER,passwd = PASSWORD)
			cur = db.cursor()
			logger.info("Retrying to update ID: %s",sql_id)
			continue
		break


def main():
	ts = time()
	result = ""
	if len(sys.argv) > 1:
		result = get_urls(TABLE,sys.argv[1])
	else:
		result = get_urls(TABLE)
	queue = Queue()

  # Create worker threads
	for x in range(8):
		worker = GetContactInfo(x,queue,db.cursor())
   	worker.daemon = True
   	worker.start()

	for data in result:
		logger.info("Queueing %s",data[0])
		queue.put((data))
  # Causes the main thread to wait for the queue to finish processing all the tasks
	queue.join()
	success_rate = (successfull / float(len(result)))*100
	logger.info('Took {}'.format(time() - ts))
	logger.info("Success Rate: %s %%",success_rate)

if __name__ == "__main__":
	main()
