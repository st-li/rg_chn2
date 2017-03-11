# -*- coding: utf-8 -*-
from __future__ import division
import re

from bs4 import BeautifulSoup
import urlparse
from urlparse import urlparse
import urllib


class DataFilter(object):
    @staticmethod
    def add_url_parameter(url, para):

        para_add = urlparse.parse_qs(para)
        url_parts = urlparse.urlparse(url)
        para_ori = urlparse.parse_qs(url_parts[4])
        for k in para_add:
            para_ori[k] = para_add[k]
        for key in para_ori:
            para_ori[key] = para_ori[key][0]

        qs = urllib.urlencode(para_ori)

        parts = (url_parts[0], url_parts[1], url_parts[2], url_parts[3], qs, url_parts[5])
        url = urlparse.urlunparse(parts)

        return url

    @staticmethod
    def simple_format(data):
        if len(data) == 0:
            return ''
        return DataFilter.trim(DataFilter.remove_linefeed(DataFilter.strip_tags(data[0]))).encode('ascii', 'ignore')

    @staticmethod
    def strip_tags(data):
        try:
            soup = BeautifulSoup(data, "lxml")
            data = soup.get_text()
        except UserWarning:
            pass
        return data

    @staticmethod
    def remove_linefeed(data):
        re_obj = re.compile("[\t\n\r]+");
        data = re_obj.sub("", data)

        return data

    @staticmethod
    def trim(data):
        data = data.strip()
        return data

    @staticmethod
    def remove_blank(data):
        pattern = re.compile(r'\s*')
        data = re.sub(pattern, '', data)
        return data
