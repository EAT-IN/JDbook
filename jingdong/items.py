# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class JingdongItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    b_cat = scrapy.Field()
    s_cate = scrapy.Field()
    s_href = scrapy.Field()
    book_img = scrapy.Field()
    book_name = scrapy.Field()
    book_author = scrapy.Field()
    book_press = scrapy.Field()
    book_publish_date = scrapy.Field()
    book_sku = scrapy.Field()
    book_price = scrapy.Field()