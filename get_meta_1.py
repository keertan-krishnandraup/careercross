import time
from helpers import get_page
import asyncio
import aiohttp
import motor.motor_asyncio
import multiprocessing
from pyquery import PyQuery as pq
import requests
import re
import json
from pprint import pprint
import logging
logging.basicConfig(filename = 'cc_meta1_log.txt', level = logging.DEBUG, filemode = 'w')
from pymongo import MongoClient


def cc_get_meta1(no_processes):
    base_url = "https://www.careercross.com/en/salary-survey?salary_prefecture=&submit_location="
    html_page = requests.get(base_url)
    pq_obj = pq(html_page.text)
    pre_options = pq_obj("#salary_prefecture").children()
    prefecture_list = []
    client = MongoClient()
    db = client['careercross']
    for i in pre_options[1:]:
        prefecture = {}
        prefecture['loc_pre'] = pq(i).attr("value")
        prefecture['name'] = pq(i).text()
        prefecture_coll = db['prefecture_meta']
        prefecture_coll.find_one_and_update({'loc_pre':prefecture['loc_pre']}, {'$set':prefecture}, upsert=True)
        prefecture_list.append(prefecture)

    table_rows = pq_obj("#site-canvas").children('div.container').children("div.row.row-offcanvas.row-offcanvas-right").\
        children("div.col-sm-9.col-md-9").children('div.table-responsive').children("table").children()[1:]
    classifications = []
    for i in table_rows:
        classification_dict = {}
        table_cols = pq(i).children()
        classification_dict['link'] = pq(table_cols[0]).children("a").attr("href")
        classification_dict['class_name'] = pq(table_cols[0]).children("a").text()
        #print(classification_dict['class_name'])
        classification_dict['class_avg_min'] = pq(table_cols[1]).text()[:-1] + 'Y'
        classification_dict['class_avg_max'] = pq(table_cols[2]).text()[:-1] + 'Y'
        classification_dict['class_avg']= pq(table_cols[3]).text()[:-1] +'Y'
        classifications.append(classification_dict)
    meta1_coll = db['meta1']
    for i in prefecture_list:
        for j in classifications:
            master_meta1_dict = {}
            master_meta1_dict['customId'] = i['loc_pre']+j['link']
            master_meta1_dict['prefecture'] = i
            master_meta1_dict['classification'] = j
            meta1_coll.find_one_and_update({'customId':master_meta1_dict['customId']}, {'$set':master_meta1_dict}, upsert=True)


if __name__=='__main__':
    PROCESSES = 8
    start = time.time()
    cc_get_meta1(PROCESSES)
    end = time.time()
    print(str(end-start), 'seconds')