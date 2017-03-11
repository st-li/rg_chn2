# -*- coding: utf-8 -*-

# Scrapy settings for ResearchGateSpider project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'ResearchGateSpider'

SPIDER_MODULES = ['ResearchGateSpider.spiders']
NEWSPIDER_MODULE = 'ResearchGateSpider.spiders'
DOWNLOAD_TIMEOUT=120

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'ResearchGateSpider (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'ResearchGateSpider.middlewares.MyCustomSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'ResearchGateSpider.middlewares.MyCustomDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

mysql_host = '127.0.0.1'
mysql_user = 'root'
mysql_passwd = 'mysql'
mysql_dbname = 'eol_flat'

mongodb_server = '118.190.45.60'
mongodb_port = 27017
mongodb_db = 'RG_Chinese'
mongodb_person_collection = 'person'
mongodb_article_collection = 'article'

mongodb_user = 'eol_spider'
mongodb_pwd = 'm~b4^Uurp)g'
mongodb_mechanism = 'SCRAM-SHA-1'

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'ResearchGateSpider.pipelines.MongoDBPipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings

HTTPCACHE_ENABLED=True
HTTPCACHE_EXPIRATION_SECS=0
HTTPCACHE_DIR='httpcache'
ignore_codes = range(100, 701)
ignore_codes.remove(200)

ignore_codes.remove(301)
ignore_codes.remove(302)
HTTPCACHE_IGNORE_HTTP_CODES = ignore_codes

HTTPCACHE_STORAGE='ResearchGateSpider.httpcache.MongoCacheStorage'
HTTPCACHE_MONGO_HOST='127.0.0.1'
HTTPCACHE_MONGO_PORT=27017
HTTPCACHE_MONGO_DATABASE="eol_spider"
HTTPCACHE_MONGO_COLLECTION="ResearchGateSpider"

HTTPERROR_ALLOWED_CODES = [301,302,200,429]


#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'


