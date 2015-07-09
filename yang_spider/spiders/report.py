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


# connection = pymongo.MongoClient(settings['MONGODB_SERVER'],
#                                  settings['MONGODB_PORT'])
# db = connection[settings['MONGODB_DB']]
# collection = db[settings['MONGODB_COLLECTION']]

crawled_urls = BloomFilter(capacity=1000*1000, error_rate=0.001)
filename = './crawleditems/report_crawled_urls.txt'
if not os.path.exists('./crawleditems'):
    os.mkdir('./crawleditems')
with open(filename, 'r') as fp:
    for url in fp:
        crawled_urls.add(url.strip())

_spider_name = 'report'


class ReportSpider(scrapy.Spider):
    name = "report"
    allowed_domains = ["hibor.com.cn"]

    # start_urls = [
    #     "http://www.hibor.com.cn/docdetail_1615977.html"
    # ]

    start_urls = [
        # "http://www.hibor.com.cn/result.asp?lm=0&area=DocTitle&timess=24&key=&dtype=&page=2200",
        # "http://www.hibor.com.cn/result.asp?lm=0&area=DocTitle&timess=24&key=&dtype=&page=3200",
        "http://www.hibor.com.cn/result.asp?lm=0&area=DocTitle&timess=24&key=&dtype=&page=9950"
    ]

    def parse(self, response):
        self.log('Parse category link: %s' % response.url, logging.DEBUG)

        sel = Selector(response)

        report_urls = []
        try:
            for request in self.find_report_page(sel, report_urls):
                yield request
        except:
            self.log('Exception in find_report_page', logging.ERROR)

        next_pages = []
        try:
            for request in self.find_next_page(sel, next_pages):
                yield request
        except:
            self.log('Exception in find_next_page', logging.ERROR)

    def parse_report(self, response):
        self.log('Parse item link: %s' % response.url, logging.DEBUG)

        sel = Selector(response)
        item = YangSpiderItem()

        item['url'] = sel.response.url
        print item['url']

        title_xpath = '//div[@class="leftn2"]//h1/span/text()'
        data = sel.xpath(title_xpath).extract()
        if len(data) != 0:
            item['title'] = data[0].encode('utf-8')
            print item['title']

        pub_date_xpath = '//div[@class="leftn2"]//tr[1]/td[3]/span/text()'
        data = sel.xpath(pub_date_xpath).extract()
        if len(data) != 0:
            item['pub_datetime'] = data[0]
            print item['pub_datetime']

        text_xpath = '//div[@class="p_main"]/p/font/text()'
        data = sel.xpath(text_xpath).extract()
        if len(data) != 0:
            item['text'] = ''.join(data).encode('utf-8')
            print item['text']

        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        item['crawl_datetime'] = now.encode('utf-8')
        print item['crawl_datetime']

        return item

    def find_report_page(self, sel, report_urls):
        requests = []
        report_url_xpath = ('//div[@class="classbaogao_sousuo_new"]//tr/td[2]'
            '/a/@href')
        data = sel.xpath(report_url_xpath).extract()
        if len(data) != 0:
            for i in data:
                report_url = "http://www.hibor.com.cn/" + i
                # result = collection.find_one({'url': report_url})
                # if result is None:
                #     logging.debug("It's so good! %s has never been crawled @_@"
                #                   % report_url)
                #     report_urls.append(report_url)
                #     crawled_urls.add(report_url)
                #     print report_url
                #     request = scrapy.Request(report_url,
                #         callback=self.parse_report)
                #     requests.append(request)
                # else:
                #     logging.debug("We have crawled %s and we'll skip it T_T"
                #                   % report_url)

                if report_url not in crawled_urls:
                    logging.debug("It's so good! %s has never been crawled @_@"
                        % report_url)
                    report_urls.append(report_url)
                    crawled_urls.add(report_url)
                    fw = open(filename, 'a+')
                    fw.write(report_url + '\n')
                    print report_url
                    request = scrapy.Request(report_url,
                        callback=self.parse_report)
                    requests.append(request)
                else:
                    logging.debug("We have crawled %s and we'll skip it T_T"
                                  % report_url)
        return requests

    def find_next_page(self, sel, next_pages):
        requests = []
        next_page_xpath = '//td/div/a/@href'
        data = sel.xpath(next_page_xpath).extract()
        if len(data) != 0:
            next_page = "http://www.hibor.com.cn/" + data[-1]
            next_pages.append(next_page)
            print next_page
            request = scrapy.Request(next_page, callback=self.parse)
            requests.append(request)
        return requests


def main():
    scrapy.cmdline.execute(argv=['scrapy', 'crawl', _spider_name])


if __name__ == '__main__':
    main()