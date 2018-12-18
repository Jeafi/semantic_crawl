from crawl_selected.spiders.common_spider import *
#精选网站：列表页js,详情页普通，一小时抓取一次
class SelectedSpiderjs1(CommonSpider):  # 需要继承scrapy.Spider类
    name= "selected_spider_js1" # 定义蜘蛛名


