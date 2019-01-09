# @Date:   2018-12-24T10:24:01+08:00
# @Email:  tang@jeffery.top
# @Last modified time: 09-Jan-2019
# @Copyright: jeafi

from readability import Document
import scrapy
import xlrd
import time
import re
from urllib.parse import urljoin
import jieba
import json
from crawl_selected.items import CrawlResultItem
from crawl_selected.repository.seed import *
from crawl_selected.repository.crawl import *
from crawl_selected.utils.article_util import *
from crawl_selected.utils.string_util import *
from crawl_selected.utils.time_util import *
# 一级页面抓取通用爬虫，该爬虫不作爬取


class AutoSpider(scrapy.Spider):  # 需要继承scrapy.Spider类
    name = "auto_spider1"  # 定义蜘蛛名
    # timestamp = time.strftime('%Y-%m-%d %H-%M-%S',time.localtime(time.time()))


    def __init__(self, name=None, **kwargs):
        super(AutoSpider, self).__init__(name=name, kwargs=kwargs)
        self.seedDB = SeedRepository()
        self.crawlDB = CrawlRepository()

    def start_requests(self):  # 由此方法通过下面链接爬取页面
        crawlName = self.name.replace("history_", "")
        seeds = self.seedDB.get_seed(crawlName)
        timestamp = time.strftime('%Y-%m-%d %H-%M-%S', time.localtime(time.time()))  # 该次爬虫的时间戳
        # seeds = self.get_seed_fromxls()
        # 从种子库的爬取
        for seed in seeds:
            meta = {}
            # meta["seedRegex"] = regex
            meta["depthNumber"] = 0
            meta["timestamp"] = timestamp
            meta["pageNumber"] = 1
            meta['is_Nextpage'] = False
            meta["seedInfo"] = seed
            meta["renderType"] = seed.renderType
            meta["pageRenderType"] = seed.pageRenderType
            meta["renderSeconds"] = seed.renderSeconds
            meta["nocontentRender"] = seed.nocontentRender
            yield scrapy.Request(url=seed.url, meta=meta, callback=self.parse)

    def parse(self, response):
        '''起始页面解析'''
        # 起始页面url抽取'''
        meta = response.meta
        start_url = meta["seedInfo"].url
        link_list = self.get_list_urls(start_url, response)
        for url in link_list:
            yield scrapy.Request(url=url, meta=meta, callback=self.parseDetail)
        if self.name.startswith("history_"):
            # 如果有下一页,爬下一页
            nextpage_urls = ArticleUtils.getNextPageUrl('', response)
            for url in nextpage_urls:
                meta['is_Nextpage']=True
                yield scrapy.Request(url=url, meta=meta, callback=self.parse)

    def parseDetail(self, response):
        '''
        详情页解析
        '''
        meta = response.meta
        url = response.url
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
        html = "".join(response.xpath("//html").extract())
        doc = Document(html)   # 利用readabilty处理文件
        if "detailData" in meta:
            detailData = meta["detailData"]
        if len(detailData) <= 0:
            detailData["title"] = doc.title()  # 详情第一页时读入标题和url
            detailData["publishAt"] = TimeUtils.get_conent_time(html)
            detailData["url"] = url
        content_snap = doc.summary()
        # 获取正文
        content = ArticleUtils.removeTag4Content(content_snap)
        ArticleUtils.mergeDict(detailData, "content", content)
        if enableDownloadImage:
            images = ArticleUtils.get_content_image_urls(content_snap, url)
            if images is not None and len(images) > 0:
                ArticleUtils.mergeDict(detailData, "contentImages", images)
        if enableDownloadFile:
            files = ArticleUtils.getContentFiles(response)
            if files is not None and len(files) > 0:
                ArticleUtils.mergeDict(detailData, "contentFiles", files)
        if enableSnapshot:
            ArticleUtils.mergeDict(detailData, "contentSnapshot", content_snap)
        # 爬取下一页
        nextpage_urls = ArticleUtils.getNextPageUrl('', response)
        if StringUtils.isNotEmpty(nextpage_urls):
            meta["detailData"] = detailData
            yield scrapy.Request(url=nextpage_urls, meta=meta, callback=self.parseDetail)
        else:
            item = ArticleUtils.meta2item(meta, detailData["url"])
            for (k, v) in detailData.items():
                itemValue = None
                if "category" == k and k in item:
                    itemValue = item[k] + "/" + v
                elif "contentImages" == k or "contentFiles" == k:
                    itemValue = json.dumps(list(v.values()), ensure_ascii=False)
                else:
                    itemValue = v
                item[k] = itemValue
            item['html'] = html

            yield item

        '''
        调试代码段，将没找到时间戳的url输出到文件调试
        '''
        # if(item["publishAt"]) == '':
        #     f = open("urlwithoutime.txt",'a+', encoding = "utf8")
        #     f. write(meta["seedInfo"]+'\n')
        # item["seed"] = meta["url"]

    # def auto_meta2item(cls, meta, url):
    #     seed = meta["seedInfo"]
    #     referer = seed.url
    #     item = CrawlResultItem()
    #     item["mediaFrom"] = seed.mediaFrom
    #     item["referer"] = referer
    #     item["url"] = url
    #     item["site"] = seed.site
    #     item["timestamp"] = meta["timestamp"]
    #     # listData = meta["listData"]
    #     item["category"] = seed.category
    #     item["urlLabel"] = seed.urlLabel.split(",")
    #     item["crawlName"] = seed.crawlName
    #     if StringUtils.isNotEmpty(seed.organization):
    #         item["organization"] = seed.organization
    #     return item

    # def get_image_urls(self, html, source_url):
    #     '''
    #     @param html:正文html
    #     @param url:本站地址
    #     @return ：图片字典dict
    #     '''
    #     replace_pattern = r'<[img|IMG].*?/>'   # img标签的正则式
    #     img_url_pattern = r'.+?src="(\S+)"'  # img_url的正则式
    #     img_url_list = []
    #     need_replace_list = re.findall(replace_pattern, html)  # 找到所有的img标签
    #     for tag in need_replace_list:
    #         url = re.findall(img_url_pattern, tag)
    #         if url != []:
    #             img_url_list.append(url[0])  # 找到所有的img_url
    #     imageDict = {}
    #     for img in img_url_list:
    #         if StringUtils.isEmpty(img):
    #             continue
    #         if img.startswith("data:"):
    #             continue
    #         # 图片以二进制格式写在网页,不需要下载
    #         url = ArticleUtils.getFullUrl(img, source_url)
    #         imageDict[url] = {"id":ArticleUtils.getArticleId(url),"contentUrl":img,"url":url}
    #     return imageDict

    # def get_time(self, response):
    #     '''
    #     提取时间,并转化为时间戳
    #     @param response
    #     @return 时间戳
    #     '''
    #     link_list =re.findall(r"((\d{4}|\d{2})(\-|\/)\d{1,2}\3\d{1,2})(\s?\d{2}:\d{2})?|(\d{4}年\d{1,2}月\d{1,2}日)(\s?\d{2}:\d{2})?" ,response.text)
    #     time_get = ''
    #     if link_list != []:
    #         time_get = link_list[0][0]
    #         for ele in link_list[0]:
    #             if time_get.find(ele) == -1:
    #                 time_get += ele
    #         time_get = TimeUtils.convert2Mill4Default(time_get,"")
    #     return time_get

    # def get_seed_fromxls(self):
    #     '''
    #     从xls中读初始url
    #     '''
    #     data = xlrd.open_workbook('seed.xls')
    #     table = data.sheet_by_name(u'未抓取链接')
    #     seeds = []
    #     for seed in table.col_values(3):
    #         seeds.append(WebSeed(seed))
    #     return seeds

    def get_list_urls(self, starturl, response):
        self.log('*******************************************')
        self.log(starturl)
        '''
        从初始页面中提取列表url
        @param starturl：初始url
        @parm response
        @return url列表
        '''
        lastname = ''
        i = 0
        a_tags = response.xpath('//a')
        self.log('-------------------------------')
        self.log('所有的链接数目', len(a_tags))
        count = 0
        href_parent = dict()

        for a_tag in a_tags:
            # print(a_tag)
            # 抽取href，过滤掉无效链接
            href = a_tag.xpath('@href').extract_first()
            if href is None:
                continue

            # 获取a标题文本内容，无内容的链接不抓取
            text = a_tag.xpath('text()').extract_first()
            if text is None:
                continue
            if len(text.strip()) == 0:
                continue

            # 相对地址绝对化
            if 'http' not in href:
                href = urljoin(starturl, href)

            # 获取父节点
            father_tag = a_tag.xpath('..')
            # print(father_tag)
            # print("********************************8")
            father_name = father_tag.xpath('local-name(.)').extract_first()
            if father_name is not None:
                father_name = '<' + father_name
                for index, attribute in enumerate(father_tag.xpath('@*'), start=0):
                    attribute_name = father_tag.xpath('name(@*[%d])' % index).extract_first()
                    father_name += ' ' + attribute_name + "=" + attribute.extract()
                    # print("a:"+attribute_name)

                father_name += '>'

                if father_name not in href_parent:
                    lastname = father_name
                    href_parent[father_name] = [(a_tag, text, len(text), href)]
                    print(father_name+":"+href)
                elif father_name == lastname or lastname.endswith(father_name) == True:
                    href_parent[lastname].append((a_tag, text, len(text), href))
                    print(lastname+":"+href)
                else:
                    father_name = str(i) + father_name
                    i = i + 1
                    href_parent[father_name] = [(a_tag, text, len(text), href)]
                    print(father_name+":"+href)
                    lastname = father_name

            # print(href_parent)

        # 返回的url列表
        final_urls = []
        # 父节点下面的链接进行算法提取列表页URL
        # 列表页文章标题字数和词数都要大于阈值
        for father_node in href_parent.keys():
            child_count = 0
            child_total_length = 0
            word_count = 0
            for child_tag, text, length, _ in href_parent[father_node]:
                child_count += 1
                # print(child_tag)
                # print('遍历数据', text, length)
                child_total_length += len(text.strip())
                # print(" ".join(jieba.cut(text.strip())))
                word_count += len(" ".join(jieba.cut(text.strip())).split(" "))
            # 链接描述平均字数和次数都大于阈值
            if child_total_length / child_count > 10.8 and word_count / child_count > 5.5 and child_count > 1:
                count += len(href_parent[father_node])
                self.log('------------------------')
                self.log("%s %d %f %f" % (father_node, child_count, child_total_length / child_count, word_count / child_count))

                for _, text, _, href in href_parent[father_node]:
                    print(text, '|', href)
                    final_urls.append(href)
                self.log('------------------------')
        self.log('-------------------------------')

        for url in final_urls:
            regex = r"(maps?)|(ads)|(adverti(s|z)e(ment))|(outerlink)|(redirect(ion)?)"
            pattern = re.compile(regex)
            if pattern.search(url):
                final_urls.pop(url)
        self.log('过滤后的链接数目', len(final_urls))
        if len(final_urls) == 0:
            final_urls = self.get_list_urls2(starturl, response)
        return final_urls

    def get_list_urls2(self, starturl, response):
        '''
        从初始页面中提取列表url
        @param starturl：初始url
        @parm response
        @return url列表
        '''
        a_tags = response.xpath('//a')
        self.log('-------------------------------')
        self.log('所有的链接数目 %d'% len(a_tags))
        count = 0
        href_parent = dict()

        for a_tag in a_tags:
            # print(a_tag)
            # 抽取href，过滤掉无效链接
            href = a_tag.xpath('@href').extract_first()
            if href is None:
                continue

            # 获取a标题文本内容，无内容的链接不抓取
            text = a_tag.xpath('text()').extract_first()
            if text is None:
                continue
            if len(text.strip()) == 0:
                continue

            # 相对地址绝对化
            if 'http' not in href:
                href = urljoin(starturl, href)

            # 获取父节点
            father_tag = a_tag.xpath('..')
            # print(father_tag)
            # print("********************************8")
            father_name = father_tag.xpath('local-name(.)').extract_first()
            if father_name is not None:
                father_name = '<' + father_name
                for index, attribute in enumerate(father_tag.xpath('@*'), start=0):
                    attribute_name = father_tag.xpath('name(@*[%d])' % index).extract_first()
                    father_name += ' ' + attribute_name + "=" + attribute.extract()
                    # print("a:"+attribute_name)

                father_name += '>'

                if father_name not in href_parent:
                    href_parent[father_name] = [(a_tag, text, len(text), href)]
                else:
                    href_parent[father_name].append((a_tag, text, len(text), href))

            # print(href_parent)

        # 返回的url列表
        final_urls = []
        # 父节点下面的链接进行算法提取列表页URL
        # 列表页文章标题字数和词数都要大于阈值
        for father_node in href_parent.keys():
            child_count = 0
            child_total_length = 0
            word_count = 0
            for child_tag, text, length, _ in href_parent[father_node]:
                child_count += 1
                # print(child_tag)
                # print('遍历数据', text, length)
                child_total_length += len(text.strip())
                # print(" ".join(jieba.cut(text.strip())))
                word_count += len(" ".join(jieba.cut(text.strip())).split(" "))
            # 链接描述平均字数和次数都大于阈值
            if child_total_length / child_count > 8 and word_count / child_count > 4 and child_count > 1:
                count += len(href_parent[father_node])
                self.log('------------------------')
                self.log(father_node, child_count, child_total_length / child_count, word_count / child_count)

                for _, text, _, href in href_parent[father_node]:
                    self.log(text+'|'+href)
                    final_urls.append(href)
                self.log('------------------------')
        self.log('-------------------------------')

        for url in final_urls:
            regex = r"(maps?)|(ads)|(adverti(s|z)e(ment))|(outerlink)|(redirect(ion)?)"
            pattern = re.compile(regex)
            if pattern.search(url):
                final_urls.pop(url)
        self.log('过滤后的链接数目', len(final_urls))
        return final_urls