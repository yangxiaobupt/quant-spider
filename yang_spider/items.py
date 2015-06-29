# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field


class YangSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    url = Field()
    title = Field()
    pub_datetime = Field()
    text = Field()
    crawl_datetime = Field()
    pass
