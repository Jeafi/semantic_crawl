from crawl_selected.spiders.auto_spider import *

#单独测试使用
class TestAutoSpider(AutoSpider):  # 需要继承scrapy.Spider类
    name= "test_auto_spider" # 定义蜘蛛名


