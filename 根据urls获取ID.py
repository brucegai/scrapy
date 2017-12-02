#author by Gai Wang
import requests
from bs4 import BeautifulSoup
import re
import math
import numpy
import time
import pandas as pd
from random import choice
from random import randint as ri
from sqlalchemy import create_engine
import gevent
import gevent.monkey
gevent.monkey.patch_socket()
p = re.compile(r"\d+")
f = re.compile(r"[0-9].[0-9]")
engine_proxy = create_engine("mysql+pymysql://root:Audaque2016.com@172.16.1.209:3306/proxy",echo=True,connect_args={'charset':'utf8'},pool_size=30)
engine_poi = create_engine("mysql+pymysql://root:Audaque2016.com@172.16.1.209:3306/dianping_beijing",echo=True,connect_args={'charset':'utf8'},pool_size=30)


USER_AGENTS = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
]

s = requests.Session()
s.keep_alive = False
s.adapters.DEFAULT_RETRIES = 10

uas = ["%E8%BE%9D%E7%84%B9%E5%BE%88%E6%8B%89%E9%A2%A8",
 "%E8%BE%3D%E8%84%B9%E5%BE%89%E6%8B%89%E3%A2%A5",
 "%E2%AE%3A%E7%84%B9%E5%BE%63%E6%8B%89%E9%A2%A8",
 "%E8%AB%9E%E7%42%B9%F4%BE%88%E6%8B%89%E9%A2%A8",
 "%E8%BE%3D%E7%84%B9%E7%BE%88%E6%2B%89%E9%A1%A8",
 "%E4%BE%9D%E7%84%B9%E5%BA%13%E6%8B%89%E3%A2%A8",
 "%E4%BE%9D%E7%52%B9%E5%BA%24%E6%6B%89%E3%A3%A4",
 "%E4%BE%9D%E8%52%B3%E5%BA%24%E6%6B%52%E2%A3%A4",
 "%E4%BE%3D%E7%23%B9%E3%BA%15%E6%6B%24%E3%A3%A2",
 "%E4%BE%3D%E7%11%B9%E3%BA%15%E8%6B%14%E3%A3%A2",
 "%E4%BE%2D%E7%11%B9%E1%BA%13%E8%6B%21%E2%A3%A3",
 "%E4%AD%2D%E7%13%B8%E2%BA%13%E8%6B%21%E2%A3%A1",
 "%E4%AB%2D%E7%21%B8%E2%BA%21%E8%6B%57%E2%A3%A2",
 "%E4%AB%2D%E7%21%B2%E1%BA%46%E8%1B%57%E5%A3%A8"]

cities=["beijing","shanghai","wuhan","guangzhou","suzhou","hangzhou","chongqing","nanjing","zhengzhou","wenzhou","changsha","guiyang"]

# 返回请求头
def get_header():
    c1="_hc.v=f%dc015-3517-e0c0-14db-bba40a0fe539.1481809308; __utma=1.1%d959688.1481809309.1481809309.1481809309.1; __utmz=1.1%d809309.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none);"%(ri(100,1000),ri(100,1000),ri(100,1000))
    c2="CNZZDATA1260865305=748073381-1482148384-http%253A%252F%252Fwww.dianping.com%252F%7C1482148384; CNZZDATA1260869652=765320676-1482150931-http%253A%252F%252Fwww.dianping.com%252F%7C1482150931; CNZZDATA1260952106=3427496751-1482150743-http%253A%252F%252Fwww.dianping.com%252F%7C1482150743; "
    c3="dper=adbf0d%d177f1655e50d44dd977f5efa72e8e1016e309d634c6d26648fd2%d;ua=%s; PHOENIX_ID=0a010%d-1596f71169c-493a100; ll=7fd06e815b796be3df%ddec7836c3df; s_ViewType=%d; JSESSIONID=B%d0E911F75A543EA7465CFA5ED6538; aburl=%d; cy=%d; cye=%s"%(ri(100,1000),ri(100,1000),choice(uas),ri(100,1000),ri(100,1000),ri(1,11),ri(100,1000),ri(1,10),ri(1,10),choice(cities))
    headers={"User-Agent":choice(USER_AGENTS),
             "Connection": "close",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Referer": "http://www.dianping.com/",
            "Cookie": (c1+c2+c3)}
    return(headers)


# 随机延时
def random_stop():
    a = abs(numpy.random.normal())
    if a > 3:
        a = 3
    elif a < 1:
        a = 1
    return time.sleep(a)

# 代理池
dl = pd.read_sql("proxys", engine_proxy)
def get_proxy():
    n = choice(range(1, len(dl.index)))
    proxy = {"http:":"http://%s:%s" %(dl["ip"][n],dl["port"][n])}
    return(proxy)


# 以一个小类的url获取全部分页的ID 有总数有ID
def get_allids_kind2(url_kind2): #参数url不带页号
    soup = BeautifulSoup(s.get(url_kind2, headers=get_header(), proxies = get_proxy()).text, "lxml")
    n = math.ceil(int(soup.select("div.section.Fix > div.bread.J_bread > span.num")[0].text.strip("()"))/15)
    n = n if n<= 50 else 50
    url_list = [url_kind2+"p"+str(i) for i in range(1,n+1)]
    ids = [p.findall(BeautifulSoup(s.get(url,headers=get_header(), proxies = get_proxy()).text,"lxml").text.split("shopIDs:")[1].split("facade")[0]) for url in url_list]
    ids = sum(ids,[])
    ids = list(set(ids))
    return(ids)

# # # 以一个小类的url获取全部分页的ID 无总数有ID
# def get_allids_kind2(url_kind2): #参数url不带页号
#     soup = BeautifulSoup(s.get(url_kind2, headers=get_header(), proxies = get_proxy()).text, "lxml")
#     try:
#         n = int(soup.select(".Pages a")[-2].text)
#     except:
#         n = 1
#     url_list = [url_kind2+"p"+str(i) for i in range(1,n+1)]
#     ids = []
#     for url0 in url_list:
#         soup0 = BeautifulSoup(s.get(url0, headers=get_header(), proxies = get_proxy()).text, "lxml")
#         ids.append(p.findall(soup0.text.split("shopIDs:")[1].split("]")[0]))
#     ids = sum(ids,[])
#     ids = list(set(ids))
#     return(ids)

# # # 以一个小类的url获取全部分页的ID 无总数无ID
# def get_allids_kind2(url_kind2): #参数url不带页号
#     soup = BeautifulSoup(s.get(url_kind2, headers=get_header(), proxies = get_proxy()).text, "lxml")
#     try:
#         n = int(soup.select(".pages a")[-2].text)
#     except:
#         n = 1
#     url_list = [url_kind2+"p"+str(i) for i in range(1, n+1)]
#     ids = []
#     for url0 in url_list:
#         soup0 = BeautifulSoup(s.get(url0, headers=get_header(), proxies = get_proxy()).text, "lxml")
#         ids.append(list(set([p.findall(i["href"])[0] for i in soup0.select(".shop-title h3 a")])))
#     ids = sum(ids,[])
#     ids = list(set(ids))
#     return(ids)



def write_ids(url):
    ids = get_allids_kind2(url)
    df = pd.DataFrame(dict(shopid=ids,url=url))
    df.to_sql(con = engine_poi, name = "feast_shopids", if_exists = 'append', flavor = "mysql",index=False)


url = pd.read_sql("feast_urls",engine_poi)["url"]

# # 异步进程池更新
# from multiprocessing import Pool
# if __name__ == '__main__':
#     pool = Pool(8)
#     pool.map_async(write_ids, url)
#     pool.close()
#     pool.join()

# 协程池
from gevent.pool import Pool
def insert_food():
    if __name__ == '__main__':
        pool = Pool(8)
        for url0 in url[0:]:
            try:
                pool.spawn(write_ids, url0)
            except:
                print("%s报错了！",url0)
#                df = pd.DataFrame(dict(url=url0),index=[0])
#                df.to_sql(con = engine_poi, name = "missed_urls", if_exists = 'append', flavor = "mysql",index=False)
        pool.join()

insert_food()

