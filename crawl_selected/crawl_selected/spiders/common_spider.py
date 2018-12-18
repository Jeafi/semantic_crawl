import scrapy
from crawl_selected.items import CrawlResultItem

from crawl_selected.repository.seed import *
from crawl_selected.repository.crawl import *
from crawl_selected.utils.article_util import *
from crawl_selected.utils.string_util import *

#一级页面抓取通用爬虫，该爬虫不作爬取
class CommonSpider(scrapy.Spider):  # 需要继承scrapy.Spider类
    name= "common_spider" # 定义蜘蛛名

    def __init__(self, name=None, **kwargs):
        super(CommonSpider,self).__init__(name=name,kwargs=kwargs)
        self.seedDB = SeedRepository()
        self.crawlDB = CrawlRepository()


    def start_requests(self):  # 由此方法通过下面链接爬取页面
        crawlName = self.name.replace("history_","")
        seeds = self.seedDB.get_seed(crawlName)
        # 定义爬取的链接
        for seed in seeds:
            regex = self.seedDB.get_regex(seed.regexName)
            if len(regex) > 0:
                meta = {}
                meta["seedRegex"] = regex
                meta["depthNumber"] = 0
                meta["pageNumber"] = 1
                meta["seedInfo"] = seed
                meta["renderType"] = seed.renderType
                meta["pageRenderType"] = seed.pageRenderType
                meta["renderSeconds"] = seed.renderSeconds
                meta["nocontentRender"] = seed.nocontentRender
                yield scrapy.Request(url=seed.url,meta=meta, callback=self.parse)
            else:
                self.log("%s no regex" % seed.url)

    def parse(self, response):
        meta = response.meta
        regexList = meta["seedRegex"]
        seed = meta["seedInfo"]
        depthNumber = int(meta["depthNumber"])
        regexDict = regexList[depthNumber].regexDict
        if "list" not in regexDict:
            self.log("%s no list regex" % response.url)
            yield
        listRegexs = regexDict["list"]
        domain = meta["seedInfo"].domain
        detailUrls = ArticleUtils.getResponseContents4WebRegex(listRegexs,response)
        listDataAll = {}
        for (k, v) in regexDict.items():
            if "nextPage" == k or "list" == k:
                continue
            itemValues = ArticleUtils.getResponseFieldValue(k, False, v, response)
            listDataAll[k] = itemValues
        listRegex = listRegexs[-1]

        isDetail = True
        if depthNumber+1 < regexList[-1].depthNumber:
            isDetail = False
        for i,detailUrl in enumerate(detailUrls):
            isVaildUrl = True
            if StringUtils.isNotEmpty(listRegex.resultFilterRegex):
                isVaildUrl = re.match(listRegex.resultFilterRegex, detailUrl)
            if not isVaildUrl:
                continue
            targetUrl = ArticleUtils.getFullUrl(detailUrl,response.url)
            if depthNumber == 0:
                targetUrl = ArticleUtils.getFullUrl(detailUrl,seed.url)
            self.logger.info("isDetail %s targetUrl %s" % (str(isDetail),targetUrl))
            # if domain not in targetUrl:
            #     continue
            listData = {}
            metaCopy = meta.copy()
            if "listData" in meta and len(meta["listData"])>0:
                listData = meta["listData"]
            for (k,v) in listDataAll.items():
                if v is not None and i<len(v) and v[i] is not None and StringUtils.isNotEmpty(str(v[i])):
                    listDataValue = v[i]
                    if "category" == k and k in listData:
                        listDataValue = listData["category"+"/"+listDataValue]
                    listData[k] = listDataValue
            metaCopy["listData"] = listData
            metaCopy["contentPageNumber"] = 1
            metaCopy["depthNumber"] = depthNumber+1
            metaCopy["refererLink"] = response.url
            metaCopy["renderType"] = listRegex.renderType
            metaCopy["pageRenderType"] = listRegex.pageRenderType
            metaCopy["renderSeconds"] = listRegex.renderSeconds
            # metaCopy["renderBrowser"] = listRegex.renderBrowser
            if ArticleUtils.isFile(targetUrl):
                self.crawlDB.saveFileCrawlDetail(metaCopy,targetUrl)
            elif isDetail:
                yield scrapy.Request(url=targetUrl,meta=metaCopy, callback=self.parseDetail)
            else:
                self.log("next level %s" % targetUrl)
                yield scrapy.Request(url=targetUrl, meta=metaCopy, callback=self.parse)

        pageNumber = meta["pageNumber"]
        maxPageNumber = 0
        nextPageRegex = []
        if "nextPage" in regexDict:
            nextPageRegex = regexDict["nextPage"]
            maxPageNumber = nextPageRegex[-1].maxPageNumber
        if self.name.startswith("history_") and ((maxPageNumber > 0 and pageNumber <= maxPageNumber) or maxPageNumber<=0):
            nextUrls = ArticleUtils.getNextPageUrl(nextPageRegex,response)
            if len(nextUrls) > 0 and StringUtils.isNotEmpty(nextUrls[0]):
                targetNextUrl = nextUrls[0]
                self.log("nextPage %s" % targetNextUrl)
                meta["pageNumber"] = meta["pageNumber"]+1
                yield scrapy.Request(url=targetNextUrl, meta=meta, callback=self.parse)
            else:
                self.log("lastPage %s" % (response.url))


    def parseDetail(self, response):
        meta = response.meta
        url = response.url
        regexList = meta["seedRegex"]
        regexDict = regexList[-1].regexDict
        seed = meta["seedInfo"]
        enableDownloadFile = False
        enableDownloadImage = False
        enableSnapshot = False
        if seed.enableDownloadFile == 1:
            enableDownloadFile = True
        if seed.enableDownloadImage == 1:
            enableDownloadImage = True
        if seed.enableSnapshot == 1:
            enableSnapshot = True
        detailData = {}
        if "detailData" in meta:
            detailData = meta["detailData"]
        if len(detailData) <=0:
            detailData["url"] = url
        maxPageNumber = 0
        pageContentSnapshot = None
        nocontentRender = meta["nocontentRender"]
        for (k, v) in regexDict.items():
            if "nextPage" == k:
                continue
            if "content" == k:
                maxPageNumber = v[-1].maxPageNumber
                if enableDownloadImage:
                    images = ArticleUtils.getContentImages(v,response)
                    if images is not None and len(images) > 0:
                        ArticleUtils.mergeDict(detailData,"contentImages",images)

                if enableDownloadFile:
                    files = ArticleUtils.getContentFiles(response)
                    if files is not None and len(files) > 0:
                        ArticleUtils.mergeDict(detailData, "contentFiles", files)


                contentSnapshots = ArticleUtils.getResponseFieldValue("contentSnapshot",True,v,response)
                if contentSnapshots is not None and len(contentSnapshots) > 0 and StringUtils.isNotEmpty(contentSnapshots[0]):
                    pageContentSnapshot = contentSnapshots[0]
                    if enableSnapshot:
                        ArticleUtils.mergeDict(detailData,"contentSnapshot",contentSnapshots[0])



            itemValues = ArticleUtils.getResponseFieldValue(k,True,v,response)
            if itemValues is not None and len(itemValues) > 0 and itemValues[0] is not None and StringUtils.isNotEmpty(str(itemValues[0])):
                ArticleUtils.mergeDict(detailData,k,itemValues[0])


        if pageContentSnapshot is None and nocontentRender == 1 and not ArticleUtils.isRender(meta,self.name):
            metaCopy = meta.copy()
            metaCopy["renderType"] = 1
            metaCopy["renderSeconds"] = 5
            metaCopy["detailData"] = detailData
            self.log("redirect url %s" % url)
            #获取不到正文，尝试使用js渲染方式，针对网站部分链接的详情页使用js跳转
            yield scrapy.Request(url=url, meta=metaCopy, callback=self.parseDetail,dont_filter=True)
        else:
            contentPageNumber = meta["contentPageNumber"]
            nextPageRegex = []
            if "nextPage" in regexDict:
                nextPageRegex = regexDict["nextPage"]
                maxPageNumber = nextPageRegex[-1].maxPageNumber
            targetNextUrl = ""
            if maxPageNumber <= 0 or (maxPageNumber > 0 and contentPageNumber < maxPageNumber):
                nextUrls = ArticleUtils.getNextPageUrl(nextPageRegex,response)
                if len(nextUrls) > 0 and StringUtils.isNotEmpty(nextUrls[0]):
                    targetNextUrl = nextUrls[0]
            if StringUtils.isNotEmpty(targetNextUrl):
                meta["detailData"] = detailData
                meta["contentPageNumber"] = contentPageNumber+1
                self.log("detail nextPage %s %s" % (str(contentPageNumber+1),targetNextUrl))
                yield scrapy.Request(url=targetNextUrl, meta=meta, callback=self.parseDetail)
            else:
                item = ArticleUtils.meta2item(meta, detailData["url"])
                for (k,v) in detailData.items():
                    itemValue = None
                    if "category" == k and k in item:
                        itemValue = item[k] + "/" + v
                    elif "contentImages" == k or "contentFiles" == k:
                        itemValue = json.dumps(list(v.values()),ensure_ascii=False)
                    else:
                        itemValue = v
                    item[k] = itemValue
                yield item


