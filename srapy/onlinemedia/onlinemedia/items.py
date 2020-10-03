# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class newsItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    text = scrapy.Field()
    website = scrapy.Field()
    index = scrapy.Field()
    comments = scrapy.Field()
    rubrics = scrapy.Field()
    paywall = scrapy.Field()