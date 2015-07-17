# -*- coding: utf-8 -*-
# author: yangxiao

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo

from scrapy.conf import settings
from scrapy.exceptions import DropItem
import logging


class YangSpiderPipeline(object):
    def process_item(self, item, spider):
        return item


class LocalFilePipeline(object):
    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            pub_datetime = item.get('pub_datetime')
            if pub_datetime is not None:
                url = item.get('url')
                doc_id = item.get('doc_id')
                title = item.get('title')
                pub_datetime = ''.join(pub_datetime.split(' ')[0].split('-'))
                print pub_datetime
                crawl_datetime = item.get('crawl_datetime')
                text = item.get('text', 'Text is Null!')
                with open('./tonghuashun/%s' % pub_datetime, 'a') as fb:
                    fb.write(url + '\001')
                    fb.write(doc_id + '\001')
                    fb.write(title.encode('utf-8').replace('\001', '').replace('\n', '') + '\001')
                    fb.write(pub_datetime.replace('\001', '') + '\001')
                    fb.write(crawl_datetime.replace('\001', '') + '\001')
                    fb.write(text.encode('utf-8').replace('\001', '').replace('\n', '') + '\n')
        return item

class MongoDBPipeline(object):
    def __init__(self):
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            # result = self.collection.find_one({'url': item['url']})
            # if result is None:
            # self.collection.insert(dict(item))
            #     logging.debug("Item added to MongoDB database -_-")
            # else:
            #     logging.debug("Item is already in the database ^_^")
            self.collection.insert(dict(item))
            logging.debug("Item has been added to MongoDB -_-")
        return item

class HDFSPipeline(object):
    def __init__(self):
        pass

    def process_item(self, item, spider):
        pass
