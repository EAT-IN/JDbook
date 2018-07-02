# -*- coding: utf-8 -*-
import scrapy
from scrapy_redis.spiders import RedisSpider  # 想要使用redis_spider首先先要导入redis的spider类
from copy import deepcopy
import json
import urllib
from ..items import JingdongItem


class JbSpider(RedisSpider):
    name = 'book'
    allowed_domains = ['jd.com', 'p.3.cn']  # 这里需要注意的是 我们抓取的网站会跳到另一个网址进行查询price 所以需呀加上网址规则防止被过滤
    # start_urls = ['https://book.jd.com/booksort.html']
    redis_key = "jingdong"  # 使用redis分布式爬虫地址是要采用redis队列的形式进行修改
    def parse(self, response):
        dt_list = response.xpath('//div[@class="mc"]/dl/dt')  # 先抓取到所有的标题为基准，因为dt和dd是同级 就先抓取一个
        for dt in dt_list:
            item = JingdongItem()
            item['b_cat'] = dt.xpath('./a/text()').extract_first()  # 抓取大标题
            em_list = dt.xpath("./following-sibling::dd[1]/em")  # 抓取当前dt标签的兄弟标签的dd下面的所有的em标签
            for em in em_list:
                item['s_cate'] = em.xpath('./a/text()').extract_first()  # 抓取小标题
                item['s_href'] = em.xpath('./a/@href').extract_first()  # 抓取小标题的链接
                if item['s_href'] is not None:
                    item['s_href'] = "https:" + item['s_href']
                    yield scrapy.Request(
                        item['s_href'],
                        callback=self.parse_book_list,
                        meta={"item": deepcopy(item)}
                    )

    def parse_book_list(self, response):
        """这里是街上上面传来的数据进行图书详情页的爬取"""
        item = response.meta['item']  # 接收上面传来的字典
        li_list = response.xpath('//ul[@class="gl-warp clearfix"]/li')  # 获取到每一个图书列表的li标签
        for li in li_list:
            item["book_img"] = li.xpath(".//div[@class='p-img']//img/@src").extract_first()  # 获取到图书的img地址
            if item["book_img"] is None:
                item["book_img"] = li.xpath(
                    ".//div[@class='p-img']//img/@data-lazy-img").extract_first()  # 如有的url地址规则和上面的不一样
            item["book_img"] = "http:" + item["book_img"] if item["book_img"] is not None else None
            item["book_name"] = li.xpath(".//div[@class='p-name']/a/em/text()").extract_first().strip()  # 获取图书的名字
            item["book_author"] = li.xpath('.//span[@class="author_type_1"]/a/text()').extract()  # 获取到书的作者,作者可能有多位
            item["book_press"] = li.xpath(".//span[@class='p-bi-store']/a/@title").extract_first()  # 出版社信息
            item["book_publish_date"] = li.xpath(".//span[@class='p-bi-date']/text()").extract_first().strip()  # 发布日期
            item["book_sku"] = li.xpath("./div/@data-sku").extract_first()  # 获取到图书的sku信息 相当于图书的编号,因为图书的详情页需要sku进行拼接
            yield scrapy.Request(
                "https://p.3.cn/prices/mgets?skuIds=J_{}".format(item["book_sku"]),  # 价格是通过Js生成的 通过在network里面搜索price查找到
                callback=self.parse_book_prise,
                meta={"item": deepcopy(item)}
            )
            next_url = response.xpath('//a[@class="pn-next"]/@href').extract_first()  # 获取下一页的链接
            if next_url is not None:
                next_url = urllib.parse.urljoin(response.url, next_url)  # 获取到的下一页的链接需要补全才能访问
                yield scrapy.Request(
                    next_url,
                    callback=self.parse_book_list,
                    meta={"item": deepcopy(item)}
                )

    def parse_book_prise(self, response):
        """这里负责把价格提取出来，并添加到字典里面去"""
        item = response.meta["item"]  # 先接收一下字典
        item["book_price"] = json.loads(response.body.decode())[0]["op"]
        yield item
