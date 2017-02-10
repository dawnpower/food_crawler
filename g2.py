import os,sys
from icrawler import Downloader
#from icrawler.builtin import GoogleImageCrawler
from google import GoogleImageCrawler
import datetime
from mysqldb import mysqlhelper
#import redis

#r = redis.StrictRedis(host='localhost', port=6379, db=0)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sqlhelper = mysqlhelper()
sqlhelper.select_db('test')

def getlist():
    a = []
    f = open('foodlist.txt','r')
    for line in f.readlines():
        a.append(line.strip('\n'))
    return a

def crawl():
    foodlist = getlist()
    for item in foodlist:
        root_dir = os.path.join('image',item)
        google_crawler = GoogleImageCrawler(parser_threads=2,downloader_threads=4,storage={'root_dir':root_dir})
        google_crawler.crawl(keyword=item, offset=0, max_num=5,
                                  date_min=None, date_max=None,
                                                       min_size=(200,200), max_size=None)

def test_crawl():
    root_dir = os.path.join('test','override')
    google_crawler = GoogleImageCrawler(parser_threads=2,downloader_threads=4,storage={'root_dir':root_dir})
    google_crawler.crawl(keyword="chilli crab", offset=0, max_num=2,
                              date_min=None, date_max=None,
                                                   min_size=(200,200), max_size=None)

test_crawl()
#crawl()

