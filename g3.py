import os,sys
from icrawler import Downloader
from googlehelper import GoogleImageCrawler
import datetime
from mysqldb import mysqlhelper
from datetime import date
from flickrhelper import FlickrImageCrawler
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sqlhelper = mysqlhelper()
sqlhelper.select_db('test')

def getlist():
    a = []
    f = open('foodlist.txt','r')
    for line in f.readlines():
        a.append(line.strip('\n'))
    return a

def crawlflickr():
    foodlist = getlist()
    for item in foodlist:
	root_dir = os.path.join('flickrimage',item)
	if os.path.exists(root_dir):
	    print root_dir
	    pass
	else:
	    print root_dir
	    os.makedirs(os.path.join('.',root_dir))
	flickr_crawler = FlickrImageCrawler('bbb67c5784518ff2a7c3db49729626fd',storage={'root_dir':root_dir})
	flickr_crawler.crawl(max_num=250,text=item)    

crawlflickr()
