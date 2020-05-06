from lxml import etree
from itertools import cycle
from apscheduler.schedulers.blocking import BlockingScheduler
# from log_helper import write_log
from aiohttp import ClientSession
from urllib import parse
import asyncio
import requests
import hashlib
import re
import time
import datetime
import json

#账号密码
ul = cycle([])

scheduler = BlockingScheduler()

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
}
# post_headers = {"Content-Type": "application/json"}

login_url = 'https://www.zhuanyes.com/member.php?mod=logging&action=login&loginsubmit=yes&infloat=yes&lssubmit=yes&inajax=1'
jingdou_url = 'https://www.zhuanyes.com/jingdou/'
post_url = 'http://cbd-log.smzdm.com:8083/event/sync/JingBean/V1?dept=bj_clt&iscap=false&cluster=bj'
# 公司代理
proxy = cycle([

])


def download_haozhuan(u):
    data = {
        'username': u['username'],
        'password': u['password'],
        'cookietime': '2592000',
        'fastloginfield': 'username',
        'handlekey': '1s',
        'quickforward': 'yes'
    }
    session = requests.session()
    session.post(login_url, data=data, headers=headers, verify=False)
    res = session.get(jingdou_url, verify=False)
    print(data)
    return res.text


def download_jingdong(url):
    try:
        res = requests.get(url, headers=headers, proxies={'all': next(proxy)}, verify=False, timeout=10)
        con = res.text
    except Exception:
        return download_jingdong(url)
    return con


def get_md5_hash(string):
    m = hashlib.md5()
    m.update(string.encode())
    return m.hexdigest()


def parse_haozhuan(con):
    html = etree.HTML(con)
    href_list = html.xpath("//ul[@id='itemlist']/li/span/a/@href")
    jingdou_list = html.xpath("//ul[@id='itemlist']/li/span[@class='jingdou_prize']/text()")
    date_list = html.xpath('//ul/li/span[@class="jingdou_date"]/span/@title')
    return href_list, jingdou_list, date_list


def parse_jingdong(con):
    html = etree.HTML(con)
    p = "shopId: '(.*?)'"
    shopId = re.findall(p, con)[0]
    title = html.xpath('//title/text()')[0]
    title = title.replace('\n', '').strip()
    p = r"var IS_ZIYING_SHOP = '(\d)'"
    is_ziying = int(re.findall(p, con)[0])
    IS_ZIYING_SHOP = 'false' if is_ziying else 'true'
    return IS_ZIYING_SHOP, title, shopId


def de_weight(href_list, jingdou_list, check):
    for i in range(len(href_list)):
        if get_md5_hash(href_list[i] + jingdou_list[i]) not in check:
            check.append(get_md5_hash(href_list[i] + jingdou_list[i]))


async def handler():
    res = []
    haozhuan_con = download_haozhuan(next(ul))
    href_list, jingdou_list, date_list = parse_haozhuan(haozhuan_con)
    for i in range(len(href_list)):
        jingdong_con = download_jingdong(href_list[i])
        IS_ZIYING_SHOP, title, shopId = parse_jingdong(jingdong_con)
        now = datetime.datetime.now()
        crawl_time = now.strftime('%Y-%m-%d %H:%M:%S')
        p = "venderId=(.*)"
        venderId = re.findall(p, href_list[i])[0]
        shopUrl = href_list[i].replace('venderId', 'shopId')
        data = {
            'ShopUrl': shopUrl.replace(venderId, shopId),
            'ShopName': parse.quote(title),
            'IsThirdPartySaler': IS_ZIYING_SHOP,
            'Content': parse.quote(jingdou_list[i]),
            'UpdatedDate': date_list[i]+':00',
            'CrawlTime': crawl_time
        }
        print(data)
        log = {
            'platform': 'haozhuan', 'task': 'haozhuan',
            'function': 'haozhuan', 'url': href_list[i].replace('venderId', 'shopId'),
            "event": "E1", 'data': data
        }
        # write_log(log)
        await Post(post_url, data)
        print(i)
        res.append(data)
    print()


async def Post(url, data):
    async with ClientSession() as session:
        data = json.dumps(data, ensure_ascii=False)
        async with session.post(url, data=data, headers={"Content-Type": "application/json"}) as res:
            await res.text()


# @scheduler.scheduled_job('cron', month='*', day='*', hour='11,13,16,20,23')
def main():
    a = time.time()
    loop = asyncio.new_event_loop()
    loop.run_until_complete(handler())
    # handler()
    print(time.time() - a)


if __name__ == '__main__':
    # scheduler.start()
    print('1')
    main()