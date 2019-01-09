
import pymongo
from scrapy.utils.project import get_project_settings
from crawl_selected.utils.time_util import *
from crawl_selected.utils.annotation import *
from crawl_selected.utils.article_util import *
from crawl_selected.repository.filedownload import *

import logging

@singleton
class CrawlRepository:

    def __init__(self):
        settings = get_project_settings()
        self.client = pymongo.MongoClient(settings.get('MONGO_URI'))
        self.db = self.client[settings.get('MONGO_DB')]
        self.logger = logging.getLogger("crawlRepository")
        self.crawlDetail = "crawlDetail"
        self.crawlSnapshot = "crawlSnapshot"
        self.crawlStat = "crawlStat"
        self.downloadDB = FileDownloadRepository()
        # self.downloadFiles = "downloadFiles"

    def saveCrawlDetail(self,item):
        now = TimeUtils.getNowMill()
        detail = dict(item)
        id = ArticleUtils.getArticleId(detail["url"])
        detail["_id"] = id;
        detail["createAt"] = now
        detail["updateAt"] = now
        if "html" in detail:
            detail.pop("html")
        if "timestamp" in detail:
            detail.pop("timestamp")
        if "content" in detail:
            if "contentSnapshot" in detail:
                snapshotDetail = {"_id":id,"content":detail["contentSnapshot"],"url":detail["url"],"updateAt":now}
                self.db[self.crawlSnapshot].save(snapshotDetail)
                detail.pop("contentSnapshot")
            self.db[self.crawlDetail].save(detail)
            self.logger.info("save %s %s" % (item["url"], id))
            urls = []
            if "contentImages" in detail:
                contentImages = json.loads(detail["contentImages"])
                for img in contentImages:
                    urls.append(img["url"])
            if "contentFiles" in detail:
                contentFiles = json.loads(detail["contentFiles"])
                for fileUrl in contentFiles:
                    urls.append(fileUrl["url"])
            if len(urls) > 0 and "publishAt" in detail:
                self.downloadDB.download(urls,str(detail["publishAt"]))
            # files = ArticleUtils.getDownloadFile(urls,detail["publishAt"])
            # for file in files:
            #     self.db[self.downloadFiles].save(file)
        elif ArticleUtils.isFile(detail["url"]):
            detail["fileType"] = "file"
            self.db[self.crawlDetail].save(detail)
            if "publishAt" in detail:
                self.downloadDB.download([detail["url"]],str(detail["publishAt"]))
            # files = ArticleUtils.getDownloadFile([detail["url"]],detail["publishAt"])
            # for file in files:
            #     self.db[self.downloadFiles].save(file)
            self.logger.info("save file %s %s" % (item["url"], id))
        else :
            self.logger.info("no content %s" % (item["url"]))


    def saveFileCrawlDetail(self,meta,url):
        item = ArticleUtils.meta2item(meta,url)
        self.saveCrawlDetail(item)

    def saveCrawlStat(self, item):
        postiveItem = 0  # 标示爬取是否成功（content是否有内容）
        if item["content"] != '':
            postiveItem = 1
        condition = {'seed': item["referer"], 'time': item["timestamp"]}
        count = self.db[self.crawlStat].find_one(condition)  # 查询是否存在记录
        if count is None:
            self.db[self.crawlStat].save({'seed': item["referer"], "time": item["timestamp"], "all": 1, "success": postiveItem,
                                "html": item['html']})
        else:
            if len(item['html']) > len(count['html']):
                count['html'] = item['html']
            count['all'] += 1
            count['success'] += postiveItem
            self.db[self.crawlStat].update(condition, count)
