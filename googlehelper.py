# -*- coding: utf-8 -*-

import json

from bs4 import BeautifulSoup
from six.moves.urllib.parse import urlencode

from icrawler import Crawler, Feeder, Parser, ImageDownloader

import os,datetime,sys
from mysqldb import mysqlhelper
import logging

from threading import current_thread
from PIL import Image
from six import BytesIO
from six.moves import queue
from six.moves.urllib.parse import urlparse
from icrawler.utils import ThreadPool


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

sqlhelper = mysqlhelper()
sqlhelper.select_db('test')
#r = redis.StrictRedis(host='localhost', port=6379, db=0)

class GoogleFeeder(Feeder):

    def feed(self, keyword, offset, max_num, date_min, date_max):
        base_url = 'https://www.google.com/search?'
        for i in range(offset, offset + max_num, 100):
            cd_min = date_min.strftime('%d/%m/%Y') if date_min else ''
            cd_max = date_max.strftime('%d/%m/%Y') if date_max else ''
            tbs = 'cdr:1,cd_min:{},cd_max:{}'.format(cd_min, cd_max)
            params = dict(
                q=keyword, ijn=int(i / 100), start=i, tbs=tbs, tbm='isch')
            url = base_url + urlencode(params)
            self.out_queue.put(url)
            self.logger.debug('put url to url_queue: {}'.format(url))


class GoogleParser(Parser):

    def parse(self, response):
        soup = BeautifulSoup(response.content, 'lxml')
        image_divs = soup.find_all('div', class_='rg_meta')
        for div in image_divs:
            meta = json.loads(div.text)
            if 'ou' in meta:
                yield dict(file_url=meta['ou'])

class GoogleDownloader(ImageDownloader):
    
    def download(self, task, default_ext, timeout=5, max_retry=3, **kwargs):
        """Download the image and save it to the corresponding path.
        Args:
            task (dict): The task dict got from ``task_queue``.
            timeout (int): Timeout of making requests for downloading images.
            max_retry (int): the max retry times if the request fails.
            **kwargs: reserved arguments for overriding.
        """
        file_url = task['file_url']
        retry = max_retry
        while retry > 0 and not self.signal.get('reach_max_num'):
            try:
                response = self.session.get(file_url, timeout=timeout)
            except Exception as e:
                self.logger.error('Exception caught when downloading file %s, '
                                  'error: %s, remaining retry times: %d',
                                  file_url, e, retry - 1)
            else:
                with self.lock:
                    sql = "select * from image_crawler where url IN ('%s')"%(file_url)
                    url_result = sqlhelper.select(sql)
                if url_result:
                    print "the url is exsist"
                    break
                else:
                    print "the url is not exsist"

                    if self.reach_max_num():
                        self.signal.set(reach_max_num=True)
                        break
                    elif response.status_code != 200:
                        self.logger.error('Response status code %d, file %s',
                                          response.status_code, file_url)
                        break
                    elif not self.keep_file(response, **kwargs):
                        break
                    with self.lock:
                        self.fetched_num += 1
                        filename = self.get_filename(task, default_ext)

                        self.logger.info('image #%s\t%s', self.fetched_num, file_url)
                        #self.storage.write(filename, response.content)

                        #self.logger.info("attention !!! : this is image url:%s\t%s",filename,file_url)
                        #filename = os.path.join(os.getcwd(),self.storage.root_dir,filename)
                        #self.logger.info("path %s",filename)
                        #self.logger.info("name %s",self.storage.root_dir)
                        food_name = self.storage.root_dir
			food_name = os.path.basename(food_name)
                        image_url = file_url
                        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        sql_insert = 'insert into image_crawler(query_food_name,url,timestamp) values ("%s","%s","%s")'%(food_name,image_url,dt)
                        sqlhelper.insert(sql_insert)
                        sql_select_id = 'select LAST_INSERT_ID()'
                        last_id = sqlhelper.select(sql_select_id)
                        if last_id:
                            last_id = last_id[0][0]
                            self.logger.info("print the last-id")
                            self.logger.info(last_id)
                            imagename = str(last_id)+'.jpg'
                            self.storage.write(imagename,response.content)
                            filename = os.path.join('.',self.storage.root_dir,imagename)
                            sql_update = 'update image_crawler set image_path="%s" where id=%d '%(filename,last_id)
                            sqlhelper.update(sql_update)
                        else:
                            self.logger.error("there is no last_id record")
                    #sqlhelper.insert('insert into image_crawler(image_path,query_food_name,url,timestamp) values("%s","%s","%s","%s")'%(image_path,image_path,image_url,dt))
                    #self.logger.info("root dir %s",self.storage.root_dir)
                    break
            finally:
                retry -= 1

    def worker_exec(self,
                    max_num,
                    default_ext='',
                    queue_timeout=5,
                    req_timeout=5,
                    **kwargs):
        """Target method of workers.
        Get task from ``task_queue`` and then download files and process meta
        data. A downloader thread will exit in either of the following cases:
        1. All parser threads have exited and the task_queue is empty.
        2. Downloaded image number has reached required number(max_num).
        Args:
            queue_timeout (int): Timeout of getting tasks from ``task_queue``.
            req_timeout (int): Timeout of making requests for downloading pages.
            **kwargs: Arguments passed to the :func:`download` method.
        """
        self.max_num = max_num
        while True:
            if self.signal.get('reach_max_num'):
                self.logger.info('downloaded images reach max num, thread %s'
                                 ' is ready to exit', current_thread().name)
                break
            try:
                task = self.in_queue.get(timeout=queue_timeout)
            except queue.Empty:
                if self.signal.get('parser_exited'):
                    self.logger.info('no more download task for thread %s',
                                     current_thread().name)
        	    sqlhelper.close()
                    break
                else:
                    self.logger.info('%s is waiting for new download tasks',
                                     current_thread().name)
            except:
                self.logger.error('exception in thread %s',
                                  current_thread().name)
            else:
                self.download(task, default_ext, req_timeout, **kwargs)
                self.process_meta(task)
                self.in_queue.task_done()
        self.logger.info('thread {} exit'.format(current_thread().name))


  #  def save_redis(self,path,url):
  #      r.set(path,url)
        
class GoogleImageCrawler(Crawler):

    def __init__(self,
                 feeder_cls=GoogleFeeder,
                 parser_cls=GoogleParser,
                 downloader_cls=GoogleDownloader,
                 *args,
                 **kwargs):
        super(GoogleImageCrawler, self).__init__(
            feeder_cls, parser_cls, downloader_cls, *args, **kwargs)

    def crawl(self,
              keyword,
              offset=0,
              max_num=1000,
              date_min=None,
              date_max=None,
              min_size=None,
              max_size=None,
              file_idx_offset=0):
        if offset + max_num > 1000:
            if offset > 1000:
                self.logger.error(
                    '"Offset" cannot exceed 1000, otherwise you will get '
                    'duplicated searching results.')
                return
            elif max_num > 1000:
                max_num = 1000 - offset
                self.logger.warning(
                    'Due to Google\'s limitation, you can only get the first '
                    '1000 result. "max_num" has been automatically set to %d. '
                    'If you really want to get more than 1000 results, you '
                    'can specify different date ranges.', 1000 - offset)

        feeder_kwargs = dict(
            keyword=keyword,
            offset=offset,
            max_num=max_num,
            date_min=date_min,
            date_max=date_max)
        downloader_kwargs = dict(
            max_num=max_num,
            min_size=min_size,
            max_size=max_size,
            file_idx_offset=file_idx_offset)
        super(GoogleImageCrawler, self).crawl(
            feeder_kwargs=feeder_kwargs, downloader_kwargs=downloader_kwargs)
        #sqlhelper.close()

    def set_logger(self, log_level=logging.INFO):
        """Configure the logger with log_level."""
        logging.basicConfig(
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            level=log_level,
            stream=sys.stderr,
            filename='crawl.log',
            filemode='a'
            )
        self.logger = logging.getLogger(__name__)
        logging.getLogger('requests').setLevel(logging.WARNING)
