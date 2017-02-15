import os,sys
from mysqldb import mysqlhelper
import logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

sqlhelper = mysqlhelper()
sqlhelper.select_db('test')

logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        level=logging.DEBUG,
        stream=sys.stderr,
        filename='crawltest.log',
        filemode='a'
)
logger = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

def check_file():
    sql = "select image_path from image_crawler"
    images_path = sqlhelper.select(sql)
    for item in images_path:
        paths = str(item[0]).strip('.').split('/')
        file_path = BASE_DIR
        for i in paths:
            file_path = os.path.join(file_path,i)
        if os.path.isfile(file_path):
            continue
        else:
            print file_path
            # re-download the image from the url
            # if the image is not exsisi in the url, delete the record
