import asyncio
import aiohttp
import logging
from pymongo import MongoClient
logging.basicConfig(filename='helper_log.txt', filemode='w', level=logging.DEBUG)
import multiprocessing

async def get_page(href='',proxy=None,redo=0,request_type='GET'):
    async with aiohttp.ClientSession() as client:
        logging.info('Hitting API Url : {0}'.format(href))
        response = await  client.request('{}'.format(request_type), href, proxy=proxy)
        logging.info('Status for {} : {}'.format(href,response.status))
        if response.status!= 200 and redo < 10:
            redo = redo + 1
            logging.warning("Response Code:" + str(response.status) +"received")
            return await get_page(href=href,proxy=None, redo=redo)
        else:
            return await response.text()

def get_meta_q(db_name, coll_name):
    client = MongoClient()
    harvests_db = client[db_name]
    meta1_coll = harvests_db[coll_name]
    meta1_queue = multiprocessing.Manager().Queue()
    res = meta1_coll.find({})
    for i in list(res):
        meta1_queue.put(i)
    return meta1_queue

async def make_tasks_and_exc(meta1_queue, process_queue_size, div_factor, func):
    search_queue = asyncio.Queue()
    for i in range(process_queue_size):
        if(not meta1_queue.empty()):
            await search_queue.put(meta1_queue.get())
    print(search_queue.qsize())
    logging.info(f'Initiated async queues of {process_queue_size}')
    logging.info(f'Worker async queue size:{search_queue.qsize()}')
    #print(search_queue.qsize())
    tasks = []
    times = search_queue.qsize() // div_factor + 1
    for _ in range(times + 1):
        await asyncio.sleep(0.2)
        logging.info(f'Initiating {times} batch tasks')
        for i in range(times):
            task = asyncio.Task(func(search_queue))
            tasks.append(task)
        await asyncio.gather(*tasks)