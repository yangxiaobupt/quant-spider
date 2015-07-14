# -*- coding: utf8 -*-
# author: yangxiao

import os
import time
import pymongo
from pybloom import BloomFilter

from scrapy.conf import settings
import scrapy
import scrapy.cmdline
from scrapy import Selector
import logging

from yang_spider.items import YangSpiderItem


connection = pymongo.MongoClient("182.92.225.106", 9980)
db = connection["pydata"]
collection = db["tonghuashun_scrapy_crawlera1"]

crawled_urls = BloomFilter(capacity=1000 * 1000, error_rate=0.001)
filename = './crawleditems/tonghuashun_crawled_urls.txt'
if not os.path.exists(filename):
    if not os.path.exists('./crawleditems'):
        os.mkdir('./crawleditems')
    with open(filename, 'w') as f:
        for post in collection.find():
            print post['url']
            f.write(post['url'] + '\n')
with open(filename, 'r') as fp:
    for url in fp:
        crawled_urls.add(url.strip())

_spider_name = 'tonghuashun'


class TonghuashunSpider(scrapy.Spider):
    name = "tonghuashun"
    allowed_domains = ["10jqka.com.cn"]

    start_urls = [
        "http://yuanchuang.10jqka.com.cn/djkuaiping_list/"
    ]

    def parse(self, response):
        logging.debug('Parse category link: %s' % response.url)

        sel = Selector(response)

        item_urls = []
        try:
            for request in self.find_item_page(sel, item_urls):
                yield request
        except:
            logging.debug('Exception in find_item_page')

        next_pages = []
        try:
            for request in self.find_next_page(sel, next_pages):
                yield request
        except:
            logging.debug('Exception in find_next_page')

    def parse_item(self, response):
        logging.debug('Parse item link: %s' % response.url)

        sel = Selector(response)
        item = YangSpiderItem()

        item['url'] = sel.response.url
        print item['url']

        title_xpath = '//div[@class="art_head"]/h1/text()'
        data = sel.xpath(title_xpath).extract()
        if len(data) != 0:
            item['title'] = data[0].encode('utf-8')
            print item['title']

        pub_date_xpath = '//span[@id="pubtime_baidu"]/text()'
        data = sel.xpath(pub_date_xpath).extract()
        if len(data) != 0:
            item['pub_datetime'] = data[0][:-2]
            print item['pub_datetime']

        text_xpath = ('//div[@class="art_main"]'
                      '/p[position()>1 and position()<last()]//text()')
        data = sel.xpath(text_xpath).extract()
        if len(data) != 0:
            item['text'] = ''.join(data).encode('utf-8')
            print item['text']

        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        item['crawl_datetime'] = now.encode('utf-8')
        print item['crawl_datetime']

        return item

    def find_item_page(self, sel, item_urls):
        requests = []
        item_url_xpath = '//div[@class="list_item"]/div/h2/a/@href'
        data = sel.xpath(item_url_xpath).extract()
        if len(data) != 0:
            for i in data:
                item_url = i
                #result = collection.find_one({'url': item_url})
                #if result is None:
                #    logging.debug("It's so good! %s has never been crawled @_@"
                #                  % item_url)
                #    item_urls.append(item_url)
                #    print item_url
                #    request = scrapy.Request(item_url,
                #        callback=self.parse_item)
                #    requests.append(request)
                #else:
                #    logging.debug("We have crawled %s and we'll skip it T_T"
                #        % item_url)
                print item_url
                if item_url not in crawled_urls:
                    logging.debug("It's so good! %s has never been crawled @_@"
                                  % item_url)
                    item_urls.append(item_url)
                    print 'Crawling......', item_url
                    request = scrapy.Request(item_url, callback=self.parse_item)
                    requests.append(request)
                    crawled_urls.add(item_url)
                    fw = open(filename, 'a+')
                    fw.write(item_url + '\n')
                    fw.close()
                else:
                    logging.debug("We have crawled %s and we'll skip it T_T"
                                  % item_url)
        return requests

    def find_next_page(self, sel, next_pages):
        requests = []
        next_page_xpath = '//div[@class="list_pager"]/a[@class="next"]/@href'
        data = sel.xpath(next_page_xpath).extract()
        if len(data) != 0:
            next_page = data[0]
            next_pages.append(next_page)
            print next_page
            request = scrapy.Request(next_page, callback=self.parse)
            requests.append(request)
        return requests


def main():
    scrapy.cmdline.execute(argv=['scrapy', 'crawl', _spider_name])


if __name__ == '__main__':
    main()
