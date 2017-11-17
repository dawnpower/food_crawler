import datetime
import json
import math
import os

from six.moves.urllib.parse import urlencode

from icrawler import Crawler, Feeder, Parser, ImageDownloader
from mysqldb import mysqlhelper
import logging
from bs4 import BeautifulSoup
from six.moves.urllib.parse import urlencode

from threading import current_thread
from PIL import Image
from six import BytesIO
from six.moves import queue
from six.moves.urllib.parse import urlparse
from icrawler.utils import ThreadPool

sqlhelper = mysqlhelper()
sqlhelper.select_db('test')

class FlickrFeeder(Feeder):

    def feed(self, apikey=None, max_num=4000, **kwargs):
        if apikey is None:
            apikey = os.getenv('FLICKR_APIKEY')
            if not apikey:
                self.logger.error('apikey is not specified')
                return
        if max_num > 4000:
            max_num = 4000
            self.logger.warning(
                'max_num exceeds 4000, set it to 4000 automatically.')
        base_url = 'https://api.flickr.com/services/rest/?'
        params = {
            'method': 'flickr.photos.search',
            'api_key': apikey,
            'format': 'json',
            'nojsoncallback': 1
        }
        for key in kwargs:
            if key in ['user_id', 'tags', 'tag_mode', 'text', 'license',
                       'sort', 'privacy_filter', 'accuracy', 'safe_search',
                       'content_type', 'machine_tags', 'machine_tag_mode',
                       'group_id', 'contacts', 'woe_id', 'place_id', 'has_geo',
                       'geo_context', 'lat', 'lon', 'radius', 'radius_units',
                       'is_commons', 'in_gallery', 'is_getty', 'extras',
                       'per_page', 'page']:  # yapf: disable
                params[key] = kwargs[key]
            elif key in ['min_upload_date', 'max_upload_date',
                         'min_taken_date', 'max_taken_date']:  # yapf: disable
                val = kwargs[key]
                if isinstance(val, datetime.date):
                    params[key] = val.strftime('%Y-%m-%d')
                elif isinstance(val, (int, str)):
                    params[key] = val
                else:
                    self.logger.error('%s is invalid', key)
            else:
                self.logger.error('Unrecognized search param: %s', key)
        url = base_url + urlencode(params)
	print url
        per_page = params.get('per_page', 100)
        page = params.get('page', 1)
        page_max = int(math.ceil(max_num / per_page))
        for i in range(page, page + page_max):
            complete_url = '{}&page={}'.format(url, i)
            self.output(complete_url)
	    print complete_url
            self.logger.debug('put url to url_queue: {}'.format(complete_url))


class FlickrParser(Parser):

    def parse(self, response):
        content = json.loads(response.content.decode())
        if content['stat'] != 'ok':
            return
        photos = content['photos']['photo']
        for photo in photos:
            farm_id = photo['farm']
            server_id = photo['server']
            photo_id = photo['id']
            secret = photo['secret']
            img_url = 'https://farm{}.staticflickr.com/{}/{}_{}.jpg'.format(
                farm_id, server_id, photo_id, secret)
            yield dict(file_url=img_url, meta=photo)

class FlickrDownloader(ImageDownloader):
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


class FlickrImageCrawler(Crawler):

    def __init__(self,
                 apikey=None,
                 feeder_cls=FlickrFeeder,
                 parser_cls=FlickrParser,
                 downloader_cls=FlickrDownloader,
                 *args,
                 **kwargs):
        self.apikey = apikey
        super(FlickrImageCrawler, self).__init__(
            feeder_cls, parser_cls, downloader_cls, *args, **kwargs)

    def crawl(self, max_num=10, file_idx_offset=0, **kwargs):
        kwargs['apikey'] = self.apikey
        kwargs['max_num'] = max_num
        super(FlickrImageCrawler, self).crawl(
            feeder_kwargs=kwargs,
            downloader_kwargs=dict(
                max_num=max_num, file_idx_offset=file_idx_offset))
