import MySQLdb
import logging
import sys
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    level=logging.INFO,
    stream=sys.stderr,
    filename='crawl.log',
    filemode='a'
    )
logger = logging.getLogger(__name__)


class mysqlhelper(object):
    def __init__(self,host='localhost',port=3306,user='root',passwd=''):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        try:
            self.conn = MySQLdb.connect(host=self.host,user=self.user,passwd=self.passwd)
            self.cur = self.conn.cursor()
            self.conn.ping(True)
        except MySQLdb.Error as e:
            print("mysql error %d: %s"%(e.args[0],e.args[1]))
  
    def select_db(self,db):
        try:
            self.conn.select_db(db)
        except MySQLdb.Error as e:
            print ("mysql error at select_db %d %s"%(e.args[0],e.args[1]))

    def query(self,sql):
        try:
            n = self.cur.execute(sql)
            return n
        except MySQLdb.Error as e:
            print ("mysql error at query:%s\nSQL:%s"%(e,sql))

    def select(self,sql):
        try:
            self.cur.execute(sql)
            result = self.cur.fetchall()
            return result
        except MySQLdb.Error as e:
            print ("mysql error at select:%s\nSQL:%s"%(e,sql))
	    logger.info("mysql error at select:%s\nSQL:%s"%(e,sql))

    def insert(self,sql):
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except MySQLdb.Error as e:
            print ("mysql error at insert:%s\nSQL:%s"%(e,sql))

    def update(self,sql):
        try:
            count = self.cur.execute(sql)
            self.commit()
            return count 
        except MySQLdb.Error as e:
            print ("mysql error at update:%s\nSQL:%s"%(e,sql))

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
