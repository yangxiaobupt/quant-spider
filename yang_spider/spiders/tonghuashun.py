# -*- coding: utf8 -*-
# author: yangxiao

import time
import pymongo
import hashlib

from scrapy.conf import settings
import scrapy
import scrapy.cmdline
from scrapy import Selector
import logging

from yang_spider.items import YangSpiderItem


connection = pymongo.MongoClient("182.92.225.106", 9980)
db = connection["pydata"]
collection = db["tonghuashun_docid"]

_spider_name = 'tonghuashun'


class ReportSpider(scrapy.Spider):
    name = "tonghuashun"
    allowed_domains = ["10jqka.com.cn"]

    start_urls = [
        "http://yuanchuang.10jqka.com.cn/djkuaiping_list/"
    ]

    def parse(self, response):
        self.log('Parse category link: %s' % response.url, logging.DEBUG)

        sel = Selector(response)

        item_urls = []
        try:
            for request in self.find_item_page(sel, item_urls):
                yield request
        except:
            self.log('Exception in find_report_page', logging.ERROR)

        next_pages = []
        try:
            for request in self.find_next_page(sel, next_pages):
                yield request
        except:
            self.log('Exception in find_next_page', logging.ERROR)

    def parse_item(self, response):
        self.log('Parse item link: %s' % response.url, logging.DEBUG)

        sel = Selector(response)
        item = YangSpiderItem()

        item['url'] = sel.response.url
        print item['url']

        item['doc_id'] = hashlib.md5(sel.response.url).hexdigest()
        print item['doc_id']

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
                result = collection.find_one({'url': item_url})
                if result is None:
                    logging.debug("It's so good! %s has never been crawled @_@"
                                  % item_url)
                    item_urls.append(item_url)
                    print item_url
                    request = scrapy.Request(item_url,
                        callback=self.parse_item)
                    requests.append(request)
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