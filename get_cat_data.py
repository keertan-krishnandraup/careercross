import time
from helpers import get_page, get_meta_q, make_tasks_and_exc
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

async def issue_insertd2(conn, ele):
    if (not ele):
        return
    conn.find_one_and_update({'customId':ele['customId']}, {'$set':ele}, upsert=True)

async def hit_d3_and_store(search_queue):
   if(search_queue.empty()):
       return
   q_item = await search_queue.get()
   hit_url = 'https://www.careercross.com/en/salary-survey/prefecture/' + q_item['loc_pre']
   page_html = await get_page(hit_url)
   pq_obj = pq(page_html)
   client = motor.motor_asyncio.AsyncIOMotorClient()
   db = client['careercross']
   data_coll = db['categories_data']
   table_rows = pq_obj("#site-canvas").children('div.container').children("div.row.row-offcanvas.row-offcanvas-right"). \
        children("div.col-sm-9.col-md-9").children('div.table-responsive').children("table").children()[1:]
   for i in table_rows:
       cols = pq(i).children()
       cat_dict = {}
       print(pq(cols[0]).text())
       cat_dict['category name'] = pq(cols[0]).text()
       cat_dict['avg-min'] = pq(cols[1]).text()
       cat_dict['avg-max'] = pq(cols[2]).text()
       cat_dict['avg'] = pq(cols[3]).text()
       cat_dict['meta'] = q_item
       cat_dict['customId'] = q_item['loc_pre'] + cat_dict['category name']
       cat_dict['prefecture'] = q_item['name']
       await issue_insertd2(data_coll, cat_dict)

async def make_tasks(meta1_queue, func):
    search_queue = asyncio.Queue()
    for i in range(meta1_queue.qsize()):
        if(not meta1_queue.empty()):
            await search_queue.put(meta1_queue.get())
    #print(search_queue.qsize())
    logging.info(f'Worker async queue size:{search_queue.qsize()}')
    #print(search_queue.qsize())
    tasks = []
    for i in range(search_queue.qsize()):
        task = asyncio.Task(func(search_queue))
        tasks.append(task)
    await asyncio.gather(*tasks)


def cc_driverd3(meta1_queue):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(make_tasks(meta1_queue, hit_d3_and_store))

def cat_data():
    cc_meta1a_queue = get_meta_q('careercross', 'prefecture_meta')
    print(cc_meta1a_queue.qsize())
    cc_driverd3(cc_meta1a_queue)

if __name__=='__main__':
    start = time.time()
    cat_data()
    end = time.time()
    print(str(end-start), ' seconds')