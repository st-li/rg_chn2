import pymongo
import redis
import json
from scrapy.http import Headers
from scrapy.utils.request import request_fingerprint
from scrapy.responsetypes import responsetypes
from time import time
import StringIO
import gzip

from w3lib.http import headers_dict_to_raw, headers_raw_to_dict


class RedisCacheStorage(object):

    def __init__(self, settings):
        self.redis_host = settings['HTTPCACHE_REDIS_HOST']
        self.redis_port = settings['HTTPCACHE_REDIS_PORT']
        self.separator = settings['HTTPCACHE_REDIS_SEPARATOR']

    def open_spider(self, spider):
        self.conn = redis.Redis(self.redis_host, self.redis_port)
        self.name = spider.name

    def close_spider(self, spider):
        pass

    def retrieve_response(self, spider, request):
        key = request_fingerprint(request)
        value = self.conn.hget(self.name, key)
        if not value:
            return
        value_arr = value.split(self.separator)
        stored_data = json.loads(value_arr[0])
        metadata = stored_data['metadata']
        body = str(value_arr[2])
        rawheaders = stored_data['response_headers']
        url = str(metadata['response_url'])
        status = str(metadata['status'])
        headers = Headers(rawheaders)
        respcls = responsetypes.from_args(headers=headers, url=url)
        response = respcls(url=url, headers=headers, status=status, body=body)
        return response

    def store_response(self, spider, request, response):
        """Store the given response in the redis."""
        key = request_fingerprint(request)
        stored_data = {
            'metadata': {
                'url': request.url,
                'method': request.method,
                'status': response.status,
                'response_url': response.url,
                'timestamp': time(),
            },
            'response_headers': response.headers,
            'request_headers': request.headers,
        }
        value = json.dumps(stored_data)
        value += self.separator + request.body
        value += self.separator + response.body
        self.conn.hset(self.name, key, value)




class MongoCacheStorage(object):

    def __init__(self, settings):
        self.mongo_host = settings['HTTPCACHE_MONGO_HOST']
        self.mongo_port = settings['HTTPCACHE_MONGO_PORT']
        self.mongo_db = settings['HTTPCACHE_MONGO_DATABASE']
        self.mongo_user = settings["HTTPCACHE_MONGO_USER"] if "HTTPCACHE_MONGO_USER" in settings else None
        self.mongo_pwd = settings["HTTPCACHE_MONGO_PWD"] if "HTTPCACHE_MONGO_PWD" in settings else None
        self.mongo_mechanism = settings["HTTPCACHE_MONGO_MECHANISM"] if "HTTPCACHE_MONGO_MECHANISM" in settings else None
        self.collection = settings['HTTPCACHE_MONGO_COLLECTION']        

    def open_spider(self, spider):
        self.conn = pymongo.MongoClient(self.mongo_host, self.mongo_port)
        self.db = self.conn[self.mongo_db]
        if self.mongo_user and self.mongo_pwd and self.mongo_mechanism:
            self.db.authenticate(name=self.mongo_user, password=self.mongo_pwd, mechanism=self.mongo_mechanism)
        # self.collection = self.db[self.collection]

    def close_spider(self, spider):
        self.conn.close()
        pass

    def retrieve_response(self, spider, request):
        key = request_fingerprint(request)
        collection_index = int(key, 16) % 1000
        collection_name = 'collection' + str(collection_index)
        collection = self.db[collection_name]
        print "-----------------Read cache from %s------------------" % collection_name 
        value = collection.find_one({"_id": key})
        if not value:
            return
        stored_data = value["value"]
        metadata = stored_data['metadata']
        url = str(metadata['response_url'])
        rawheaders = stored_data['response_headers']
        rawheaders = headers_raw_to_dict(rawheaders)
        if "Content-Encoding" in rawheaders:
            del rawheaders["Content-Encoding"]
        body = stored_data["response_body"]
        body = body.encode("utf-8", "w3lib_replace")
        status = str(metadata['status'])
        headers = Headers(rawheaders)
        respcls = responsetypes.from_args(headers=headers, url=url)
        response = respcls(url=url, headers=headers, status=status, body=body)
        return response

    def store_response(self, spider, request, response):
        """Store the given response in the mongo."""
        key = request_fingerprint(request)
        response_headers = headers_dict_to_raw(response.headers)
        response_body = self._get_body(response.headers, response.body)
        request_headers = headers_dict_to_raw(request.headers)
        request_body = self._get_body(request.headers, request.body)
        stored_data = {
            'metadata': {
                'url': request.url,
                'method': request.method,
                'status': response.status,
                'response_url': response.url,
                'timestamp': time(),
            },
            'response_headers': response_headers,
            'response_body': response_body,
            'request_headers': request_headers,
            'request_body': request_body,
        }
        #print stored_data

        collection_index = int(key, 16) % 1000
        collection_name = 'collection' + str(collection_index)
        collection = self.db[collection_name]

        try:
            collection.insert({"_id": key, "value": stored_data})
            print "-----------------Write cache %s------------------" % collection_name
        except Exception, e:
            print e.message
            pass

    @staticmethod
    def _get_body(headers, body):
        if "Content-Encoding" in headers and headers["Content-Encoding"] == "gzip":
            compressedstream = StringIO.StringIO(body)
            gzipper = gzip.GzipFile(fileobj=compressedstream)
            body = gzipper.read()
        else:
            body = body
        return body
