from crawl_selected.utils.http_util import *
from crawl_selected.utils.string_util import *
from crawl_selected.utils.annotation import *
from scrapy.utils.project import get_project_settings
import logging

class WebRegex:
    def __init__(self, jsonData):
        self.regexType = str(jsonData["regexType"])
        self.regexField = str(jsonData["regexField"])
        self.regexContent = str(jsonData["regexContent"])
        self.resultFormat = str(jsonData["resultFormat"])
        self.renderType = int(jsonData["renderType"])
        self.pageRenderType = int(jsonData["pageRenderType"])
        self.renderSeconds = int(jsonData["renderSeconds"])
        self.regexSort = int(jsonData["regexSort"])
        self.maxPageNumber = int(jsonData["maxPageNumber"])
        self.depthNumber = int(jsonData["depthNumber"])
        self.resultFilterRegex = str(jsonData["resultFilterRegex"])

    def setRegexContent(self,regexField,regexContent):
        self.regexField = regexField
        self.regexContent = regexContent

    def __str__(self):
        return StringUtils.dict2Json(self.__dict__)


class CrawlRegex:
    def __init__(self, depthNumber,regexDict):
        self.depthNumber = depthNumber
        self.regexDict = regexDict

    def __str__(self):
        return StringUtils.dict2Json(self.__dict__)


class WebSeed:
    def __init__(self,jsonData):
        self.urlLabel = jsonData["label"]
        self.mediaFrom = jsonData["mediaFrom"]
        self.category = jsonData["category"]
        self.regexName = jsonData["regexName"]
        self.crawlName = jsonData["crawlName"]
        self.renderType = int(jsonData["renderType"])
        self.pageRenderType = int(jsonData["pageRenderType"])
        self.renderSeconds = int(jsonData["renderSeconds"])
        self.nocontentRender = int(jsonData["nocontentRender"])
        self.site = jsonData["site"]
        self.domain = jsonData["domain"]
        self.url = str(jsonData["url"])
        self.organization = jsonData["organization"]
        self.urlMethod = str(jsonData["urlMethod"])
        self.enableDownloadFile = int(jsonData["enableDownloadFile"])
        self.enableDownloadImage = int(jsonData["enableDownloadImage"])
        self.enableSnapshot = int(jsonData["enableSnapshot"])
        self.depthCount = int(jsonData["depthCount"])

    def __str__(self):
        return StringUtils.dict2Json(self.__dict__)

@singleton
class SeedRepository:
    def __init__(self):
        settings = get_project_settings()
        self.logger = logging.getLogger("seedRepository")
        self.seedUrl = 'http://information-db-service:31001/seed/crawl?urlStatus=1&crawlName=%s&ps=100000'
        self.seedRegexUrl = 'http://information-db-service:31001/regex?regexName=%s&ps=100000'
        # self.seedUrl = settings.get("SEED_URL")
        # self.seedRegexUrl = settings.get("SEED_REGEX_URL")
        self.logger.info("init seedUrl:%s,regexUrl:%s" % (self.seedUrl,self.seedRegexUrl))


    def get_seed(self,crawlName):
        url = self.seedUrl % crawlName
        self.logger.info("get_seed %s" % url)
        jsonResult = HttpUtils.getUrl(url)
        list = jsonResult["data"]["list"]
        result = []
        for d in list:
            seed = WebSeed(d)
            result.append(seed)
        self.logger.info("%s seedUrl count %d" % (crawlName,len(result)))
        return result


    def get_regex(self,regexName):
        url = self.seedRegexUrl % regexName
        crawlRegexDict = {}
        self.logger.info("get_regex %s" % url)
        jsonResult = HttpUtils.getUrl(url)
        list = jsonResult["data"]["list"]
        for d in list:
            regex = WebRegex(d)
            depthNumber = regex.depthNumber
            crawlRegex = None
            regexDict = {}
            if depthNumber in crawlRegexDict:
                crawlRegex = crawlRegexDict[depthNumber]
                regexDict = crawlRegex.regexDict
            else:
                crawlRegex = CrawlRegex(depthNumber,regexDict)
            regexs = []
            if regex.regexField in regexDict:
               regexs = regexDict[regex.regexField]
            regexs.append(regex)
            regexs.sort(key=lambda r:r.regexSort)
            regexDict[regex.regexField] = regexs
            crawlRegex.regexDict = regexDict
            crawlRegexDict[depthNumber] = crawlRegex
        crawlRegexList = sorted(crawlRegexDict.values(), key=lambda d: d.depthNumber)
        self.logger.info("%s seedRegexUrl count %d" % (regexName, len(crawlRegexList)))
        return crawlRegexList