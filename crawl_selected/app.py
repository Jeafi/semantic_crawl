
from scrapy.utils.conf import get_config
from crawl_selected.utils.http_util import *
from scrapy.utils.project import get_project_settings
import time
import schedule
import datetime
# import logging
# import sys

# logger = None

def runSpider(url,projectName,spiderName):
    query={'project':projectName,'spider':spiderName}
    # logger.info("start run %s %s %s" %(url,projectName,spiderName))
    now = datetime.datetime.now()
    print("%s start run %s %s %s" %(now,url,projectName,spiderName))
    # sys.stderr.write("%s start run %s %s %s" %(now,url,projectName,spiderName))
    requestUrl = url+"schedule.json"
    result = HttpUtils.postQuery(requestUrl,query)
    # logger.info(result)
    now = datetime.datetime.now()
    print("%s %s" % (now,result))
    # sys.stderr.write("%s %s" % (now,result))


def selected_spider1(url,project):
    runSpider(url,project,"selected_spider1")


def selected_spider_js(url, project):
    runSpider(url, project, "selected_spider_js1")
    runSpider(url, project, "selected_spider_js2")


def official_spider1(url, project):
    runSpider(url, project, "official_spider1")
    runSpider(url, project, "policy_spider")
    runSpider(url, project, "policy_spider_js3")

def official_spider_js(url, project):
    runSpider(url, project, "official_spider_js1")
    runSpider(url, project, "official_spider_js2")
    runSpider(url, project, "policy_spider_js1")
    runSpider(url, project, "policy_spider_js2")



def test(url, project):
    print("%s %s" % (url,project))
    # logger.info("%s %s" % (url,project))

if __name__ == '__main__':
    # logging.basicConfig(filename="/home/yhye/python_project/gemantic-python/crawl_selected/log.conf")
    # logger = logging.getLogger("main")
    cfg = get_config()
    url = cfg.get('deploy', "url")
    settings = get_project_settings()
    project = settings.get("BOT_NAME")
    # logger.info("start schedule %s %s" % (url,project))
    now = datetime.datetime.now()
    # sys.stderr.write("%s start schedule %s %s" % (now,url, project))
    print("%s start schedule %s %s" % (now,url, project))
    # schedule.every().minute.do(test,url,project)
    schedule.every().hour.do(selected_spider1, url,project)
    schedule.every(3).hours.do(selected_spider_js, url, project)
    schedule.every(6).hours.do(official_spider1, url, project)
    schedule.every(8).hours.do(official_spider_js, url, project)

    while True:
        schedule.run_pending()
        time.sleep(1)
