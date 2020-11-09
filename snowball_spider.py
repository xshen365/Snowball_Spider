import os
from aip import AipNlp
import requests
from bs4 import BeautifulSoup
import json
import asyncio
import re
import time
from datetime import tzinfo, timedelta, datetime
from time import strftime
import pytz
import psycopg2
from random import randint
from snowball_sql import *


class AipNlp_API:
    def __init__(self, filepath):
        api_info = []
        self.file_path = filepath
        with open(self.file_path, 'r+') as f:
            content = f.readlines()
            for line in content:
                api_info.append(line.split('= ')[1].rstrip())
        self.APP_ID = api_info[0]
        self.API_KEY = api_info[1]
        self.SECRET_KEY = api_info[2]

    def connect_to_AipNlp(self):
        client = AipNlp(self.APP_ID, self.API_KEY, self.SECRET_KEY)
        return client

class Snowball:
    def __init__(self, stock_code):
        self.stock_code = stock_code
        self.url = 'https://xueqiu.com/statuses/search.json?count=10&comment=0&symbol={s}&hl=0&source=all&sort=&page={p}&q=&type=0'
#         self.headers = {
#             'Accept': '*/*',
#             'Origin': 'https://xueqiu.com',
#             'Accept-Encoding': 'gzip, deflate, br',
#             'Accept-Language': 'zh-CN,zh;q=0.9',
#             'Host': 'xueqiu.com',
#             'User-Agent': 'Mozilla/6.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
#             'Accept-Language': 'en-us',
#             'Referer': 'https://xueqiu.com/',
#             'Connection': 'keep-alive',
#             'X-Requested-With': 'XMLHttpRequest',
#             'Cookie': 'Hm_lpvt_1db88642e346389874251b5a1eded6e3=1604379376; Hm_lvt_1db88642e346389874251b5a1eded6e3=1604202386,1604376222,1604376235,1604376470; __utma=1.763449739.1603687419.1604368642.1604376147.4; __utmc=1; __utmz=1.1603687419.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); xq_a_token=db48cfe87b71562f38e03269b22f459d974aa8ae; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTYwNjk2MzA1MCwiY3RtIjoxNjA0MzcxMjU2MTIzLCJjaWQiOiJkOWQwbjRBWnVwIn0.pzKrbcqhTT7k-jxLJjFm2Ni9ad5Ss_K9EHg4unIYtu50r7LkqJMPvdE3_O1Yw397zwLhgkawgGbuvgikWwFDzC02MrappWgA0VxY2ErTpwcGV2VP0cYom5pG2zcK8VdGERRJkwJlnj6hNe863MrU-9_JTP2UzBZqKErO09xPoqA8LyIZGABsj8Lm3O1eyWX2ZmRFETfnf0xkQV9uk8mYFVTYwQ72MxfmOtsa6DDZ4QmUyEVYGH_GLJ2BZ3BfhxQtd6j58ks_ZqLomudG0mtLylysN7VfcmblmhczDMYPCY0y_m9_qf12JSo-bZupZMKySp3YE_jvbz2QdJrIs-YfuA; xq_r_token=500b4e3d30d8b8237cdcf62998edbf723842f73a; u=141604202385128; xqat=3242a6863ac15761c18a8469b89065b03bd5e164; device_id=5785bd7075b2299f2952b2750df9e5c3; s=d411m21qwo'
#         }
        self.headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0"}

    def clear_tags(self, comment):
        tags = re.compile(r"<.*?>")
        symb = re.compile(r'[$.*$]')
        at = re.compile(r'[//@.*?:回复 &nbsp;]+')
        text = re.sub(tags, '', comment)
        text = re.sub(symb, '', text)
        text = re.sub(at, '', text)
        return "".join(text.split())

    def get_page(self, url):
        tried = 0
        while True:
            time.sleep(3)
            try:
                response = requests.get(url, headers=self.headers)
                # response.encoding = response.apparent_encoding
                response.encoding = 'utf-8'
                if response.status_code == 200:
                    print("Request Success!")
                    tried = 0
                    return response.json()['list']
            except Exception as e:
                tried += 1
                if tried > 10:
                    pass
                else:
                    time.sleep(4)
                    continue

    def get_all_pages(self, count):
        page_list = []
        page_num = count  # page range to extract from
        for i in range(page_num):
            updated_url = self.url.format(s=self.stock_code, p=i + 1)
            page_response = self.get_page(updated_url)
            time.sleep(4)
            page_list.append(page_response)
        return page_list

    def find_post_objects(self, page_list):
        '''returns a comment dict object'''
        comment_list = []
        article_list = []
        for page in page_list:
            for post in page:
                if post['title'] == '':
                    comment_list.append(post)
                else:
                    article_list.append(post)
        return comment_list, article_list

    def parse_comment_text(self, comment_object_list):
        '''remove all tags'''
        comment_text_list = []
        for comment_object in comment_object_list:
            comment = self.clear_tags(comment_object['text'])
            comment_text_list.append(comment)
        return comment_text_list

    def parse_article_text(self, article_object_list):
        article_text_list = []
        for article_object in article_object_list:
            article = self.clear_tags(article_object['text'])
            article_text_list.append(article)
        return article_text_list

    def parse_single_text_block(self, comment_text):
        return self.clear_tags(comment_text)

    def comment_object_list(self, comment_list):
        comment_dict_list = []
        for comment in comment_list:
            comment_dict = {}
            comment_dict['timestamp'] = comment['created_at']
            comment_dict['date'] = datetime.fromtimestamp(comment['created_at'] / 1000.0) \
                .astimezone(pytz.timezone('Asia/Shanghai')).strftime("%Y/%m/%d %H:%M:%S")
            comment_dict['comment'] = self.parse_single_text_block(comment['text'])
            comment_dict['comment_like'] = comment['like_count']
            comment_dict_list.append(comment_dict)
        return comment_dict_list


class DB_Operations:
    def __init__(self, cur):
        self.cur = cur

    def initialize_database(self, table_name):
        self.cur.execute('SET search_path TO snowball;')
        try:
            self.cur.execute(snowball_schema_create)
        except Exception as e:
            print('Failed to create schema:', e)
        try:
            self.cur.execute(stock_table_create.format(table_name))
        except Exception as e:
            print(e, ': Failed to create table {} !'.format(table_name))

    def batch_insert_comments(self, table_name, comment_object_list, sentiment_list):
        for comment_object in comment_object_list:
            try:
                self.cur.execute(insert_new_comments.format(table_name),
                                 (
                                  comment_object['timestamp'],
                                  comment_object['date'],
                                  comment_object['comment'],
                                  comment_object['comment_like'],
                                  sentiment_list['positive_prob'],
                                  sentiment_list['negative_prob'],
                                  sentiment_list['confidence_prob'],
                                  sentiment_list['sentiment'],)
                                 )
            except Exception as e:
                print('Insertion error:', e)

    def select_comments_by_count(self, table_name, count):
        try:
            self.cur.execute(select_comments_by_count.format(table_name), (count,))
        except Exception as e:
            print('Selection by count error:', e)

    def select_comments_by_period(self, table_name, period):
        ''' This function finds comments that are released <period> ago

            <period> could be eg. 1 day, 2 minute, 3 second, 1 year etc.
        '''
        try:
            self.cur.execute(select_comments_by_period.format(table_name), period)
        except Exception as e:
            print('Selection by data error:', e)

    def find_time_last_comment(self, table_name):
        try:
            self.cur.execute(find_last_comment_time.format(table_name))
        except Exception as e:
            print('Find last comment error:', e)

    def delete_old_comments(self, table_name, period):
        try:
            self.cur.execute(delete_old_comments, (table_name, period))
        except Exception as e:
            print('Delete comments error:', e)

    def drop_table(self, table_name):
        try:
            self.cur.execute(comments_table_drop, table_name)
        except:
            print("Drop Error!")

    def find_record_number(self, table_name):
        try:
            self.cur.execute(find_total_number_records.format(table_name))
        except Exception as e:
            print("Fetch record count failed:", e)

