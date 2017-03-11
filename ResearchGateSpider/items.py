# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class RGPersonItem(Item):
    person_key = Field()
    fullname = Field()
    title = Field()
    target_sciences = Field()
    score = Field()
    co_authors = Field()
    topics = Field()
    skills = Field()
    institution = Field()
    department = Field()
    city = Field()
    province = Field()
    country = Field()

class RGArticleItem(Item):
    author_key = Field()
    article_key = Field()
    article = Field()
    