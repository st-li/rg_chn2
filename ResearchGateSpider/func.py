import datetime
from ResearchGateSpider.datafilter import DataFilter
import re




def parse_text_by_multi_content(content, delimiter):
    text = ''
    for c in content:
        text = "%s%s%s" % (text, DataFilter.simple_format(c.xpath('.').extract()), delimiter)
    text = text[:-len(delimiter)]
    return text


def get_google_spider_url(origin_url):
    google_spider_url = "https://ipv4.google.com/sorry/index?continue=%s" \
                        "&hl=zh-CN&q=EgSAx54QGIrXgsMFIhkA8aeDS5qZGSfjywVPCg6UscyzSOslsXzgMgFj" % origin_url
    #return google_spider_url
    return origin_url




