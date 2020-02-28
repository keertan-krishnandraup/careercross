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


async def hit_d2_and_store(search_queue):
    if(search_queue.empty()):
        return
    q_item = await search_queue.get()
    base_url = q_item['classification']['link']
    prefecture_str = '/prefecture/' + q_item['prefecture']['loc_pre']
    hit_url = base_url + prefecture_str
    page_html = await get_page(hit_url)
    pq_obj = pq(page_html)
    client = motor.motor_asyncio.AsyncIOMotorClient()
    working_db = client['careercross']
    data_coll = working_db['data_coll']
    table_rows = pq_obj("#site-canvas").children('div.container').children("div.row.row-offcanvas.row-offcanvas-right"). \
        children("div.col-sm-9.col-md-9").children('div.table-responsive').children("table").children()[1:]
    for i in table_rows:
        cols = pq(i).children()
        sub_cat_dict = {}
        print(pq(cols[0]).text())
        sub_cat_dict['sub-category name'] = pq(cols[0]).text()
        sub_cat_dict['avg-min'] = pq(cols[1]).text()
        sub_cat_dict['avg-max'] = pq(cols[2]).text()
        sub_cat_dict['avg'] = pq(cols[3]).text()
        sub_cat_dict['meta'] = q_item
        sub_cat_dict['customId'] = q_item['customId']
        sub_cat_dict['category'] = q_item['classification']['class_name']
        sub_cat_dict['prefecture'] = q_item['prefecture']['name']
        await issue_insertd2(data_coll, sub_cat_dict)


def cc_driverm2(process_queue_size, meta1_queue):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(make_tasks_and_exc(meta1_queue, process_queue_size, 2, hit_d2_and_store))


def get_jap_deets(no_processes):
    cc_meta1_queue = get_meta_q('careercross', 'meta1')
    print(cc_meta1_queue.qsize())
    process_queue_size = (cc_meta1_queue.qsize() // no_processes) + 1
    with multiprocessing.Pool(no_processes) as p:
        logging.info(f'Initiating {no_processes} pool workers')
        multi = [p.apply_async(cc_driverm2, (process_queue_size, cc_meta1_queue,)) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()




if __name__=='__main__':
    PROCESSES = 16
    start = time.time()
    get_jap_deets(PROCESSES)
    end = time.time()
    print(str(end-start), ' seconds')