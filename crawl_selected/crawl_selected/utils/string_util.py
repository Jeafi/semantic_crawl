# -*- coding: utf-8 -*-
import json

class StringUtils(object):

    @classmethod
    def trim(cls,str):
        if str is None:
            return ""
        str = str.strip()
        return str

    @classmethod
    def isEmpty(cls, str):
        if str is None or len(str)<=0:
            return True
        return False

    @classmethod
    def isNotEmpty(cls, str):
        return not StringUtils.isEmpty(str)

    @classmethod
    def dict2Json(cls, dictData):
        encode_json = json.dumps(dictData,ensure_ascii=False)
        return encode_json

    @classmethod
    def replaceSpecialWords(cls, str):
        if str is None:
            return ""
        dd = str.replace('\n', ' ')
        dd = dd.replace('\t', ' ')
        dd = dd.replace('\r', ' ')
        dd = dd.replace('\r\n', ' ')
        dd = dd.replace(u'\xa0', ' ')
        dd = dd.replace(u'\u3000', ' ')
        dd = dd.replace(u'\u2002',' ')
        return dd