from crawl_selected.spiders.common_spider import *
#精选网站：列表页普通,详情页普通,一小时抓取一次
class SelectedSpider1(CommonSpider):  # 需要继承scrapy.Spider类
    name= "selected_spider1" # 定义蜘蛛名


