# author by Gai Wang
import requests
from bs4 import BeautifulSoup
import re
import math
import numpy
import time
import pandas as pd
from random import choice,randint as ri
from sqlalchemy import create_engine
import gevent
import gevent.monkey
gevent.monkey.patch_socket()
p = re.compile(r"\d+")
f = re.compile(r"[0-9].[0-9]")
engine_poi=create_engine("mysql+pymysql://root:Audaque2016.com@172.16.1.209:3306/dianping_beijing",echo=True,connect_args={'charset':'utf8'},pool_size=30) # 服务器数据库
engine_proxy = create_engine("mysql+pymysql://root:Audaque2016.com@172.16.1.209:3306/proxy",echo=True,connect_args={'charset':'utf8'},pool_size=30)

s = requests.Session()
s.keep_alive = False
s.adapters.DEFAULT_RETRIES = 10


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
def get_proxy(dl):
    n = choice(range(1, len(dl.index)))
    proxy = {"http:":"http://%s:%s" %(dl["ip"][n],dl["port"][n])}
    return(proxy)


# 获取单个商家最新更新日期：
def get_last_update(shopid):
    url = "http://www.dianping.com/shopfood/%s/editmember" %shopid
    try:
        dt = s.get(url, headers= get_header(),proxies=get_proxy(dl))
        huoguo = BeautifulSoup(dt.text,"lxml")
        date = huoguo.select(".block-inner.desc-list.contribute-list.Fix span")[-1].text
        date = "20" + date[-8:]
        return date
    except:
        return("")

def get_position(shopid):
    url = "http://m.dianping.com/shop/%s/map"%shopid
    cd = s.get(url,headers= get_header(), proxies=get_proxy(dl))
    cd.encoding="utf-8"
    if cd.status_code == 200 and cd.text.find("请输入验证码") <1:
        soup = BeautifulSoup(cd.text, "lxml")
        try:
            long = re.findall(re.compile("(11[5-8]\.\d+)"), soup.text)[0]
        except:
            long = ""
        try:
            lat = re.findall(re.compile("(39\.\d+|4[0-1]\.\d+)"), soup.text)[0]
        except:
            lat = ""
        position = [long,lat]
        return(position)
    else:
        print("暂时爬不了经纬度！")

# 获取单个商家信息，并整合成dict：
def get_iteminfo(shopid):
    url = "http://www.dianping.com/shop/" + shopid
    cd = s.get(url,headers= get_header(), proxies=get_proxy(dl))
    cd.encoding="utf-8"
    if cd.status_code == 200 and cd.text.find("请输入验证码") <1:
        soup = BeautifulSoup(cd.text, "lxml")
        try:
            area = soup.select("div.breadcrumb > a")[1].text.strip()
        except:
            area = ""
        try:
            kind1 = soup.select("div.breadcrumb > a")[2].text.strip()
        except:
            kind1 = ""
        try:
            name = soup.select("div.breadcrumb > span")[0].text.strip()
        except:
            name = ""
        try:
            mean_score = p.findall((soup.select(".brief-info span")[0]["class"][1]))[0]
        except:
            mean_score = 0
        if str(soup.select(".brief-info > span")).find("评论") > 1:
            try:
                comments_num = soup.select(".brief-info > span")[1].text.strip("条评论")
            except:
                comments_num = 0
            try:
                mean_price = p.findall(str(soup.select(".brief-info > span")[2].text))[0]
            except:
                mean_price = ""
            try:
                factor1 = f.findall(str(soup.select(".brief-info > span")[3].text))[0]
            except:
                factor1 = ""
            try:
                factor2 = f.findall(str(soup.select(".brief-info > span")[4].text))[0]
            except:
                factor2 = ""
            try:
                factor3 = f.findall(str(soup.select(".brief-info > span")[5].text))[0]
            except:
                factor3 = ""
        else:
            comments_num = 0
            try:
                mean_price = p.findall(str(soup.select(".brief-info > span")[1].text))[0]
            except:
                mean_price = ""
            try:
                factor1 = f.findall(str(soup.select(".brief-info > span")[2].text))[0]
            except:
                factor1 = ""
            try:
                factor2 = f.findall(str(soup.select(".brief-info > span")[3].text))[0]
            except:
                factor2 = ""
            try:
                factor3 = f.findall(str(soup.select(".brief-info > span")[4].text))[0
                ]
            except:
                factor3 = ""
        try:
            address = soup.select("div.expand-info.address > span.item")[0].text.strip()
        except:
            address = ""
        try:
            longitude = re.findall(re.compile("(11[5-8]\.\d+)"), soup.text)[0]
            latitude = re.findall(re.compile("(39\.\d+|4[0-1]\.\d+)"), soup.text)[0]
        except:
            longitude = get_position(shopid)[0]
            latitude = get_position(shopid)[1]
        try:
            last_update = get_last_update(shopid)
        except:
            last_update = ""
        try:
            is_open = soup.select(".shop-closed") == []
        except:
            is_open = ""
        info = dict(shopid = shopid,
                    area=area,
                    kind1=kind1,
                    name = name,
                    mean_score = mean_score,
                    comments_num = comments_num,
                    mean_price = mean_price,
                    factor1 = factor1,
                    factor2 = factor2,
                    factor3 = factor3,
                    address = address,
                    longitude = longitude,
                    latitude = latitude,
                    last_update = last_update,
                    is_open = is_open,
                    scrapy_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        return(info)
    else:
        r = requests.get(url, headers=get_header(), proxies=get_proxy())
        if re.findall(re.compile("验证码"), r.text):
            time.sleep(random.uniform(60,180))
            try:
                r = requests.get(url, headers=get_header(), proxies=get_proxy())
                if re.findall(re.compile("验证码"), r.text):
                    raise Verification_Code_Fail
                else:
                    return(r.text)
            except:
                raise Verification_Code_Fail


def write_sql(shopid):
    try:
        df = pd.DataFrame(dict(get_iteminfo(shopid)),index=[0])
        df.to_sql(con = engine_poi, name = "lifeservice_history", if_exists = 'append', flavor = "mysql",index=False)
    except:
        print("%s爬不了，请检查！"%shopid)

# 协程池
from gevent.pool import Pool
def insert_food(remained_id):
    if __name__ == '__main__':
        pool = Pool(8)
        for id in remained_id:
            pool.spawn(write_sql, id)
        pool.join()

# # 检查全部ID是否需要更新，加入需要更新的ID
# def add_newids(shopid):
#     if get_last_update(shopid) > list(gotten_poi.last_update.loc[gotten_poi.shopid==shopid])[0]:
#         remained_id.add(shopid)
#         print("成功插入%s"%shopid)
#     else:
#         print("%s无需更新"%shopid)
#
#
# def insert_newids():
#     if __name__ == '__main__':
#         pool = Pool(8)
#         for id in total_ids:
#             pool.spawn(add_newids, id)
#         pool.join()
#
# # 清洗表数据
# def clean_tb(tb_name):
#     engine_poi.execute("SET SQL_SAFE_UPDATES = 0;")
#     engine_poi.execute("DELETE FROM %s WHERE longitude < 113 OR longitude > 115 OR latitude < 22 OR latitude > 23;"%tb_name)
#
# # 从history表中提取latest表
# def get_latest(kind_name):
#     df = pd.read_sql("%s_history"%kind_name, engine_poi)
#     df = df.sort("last_update", ascending=False)
#     df = df.drop_duplicates(["shopid"])
#     df.to_sql(con = engine_poi, name = "%s_latest"%kind_name, if_exists = 'replace', flavor = "mysql",index=False)
#
# while True:
#     gotten_poi = pd.read_sql("lifeservice_latest",engine_poi)
#     dl = pd.read_sql("proxys", engine_proxy)
#     total_ids = set(pd.read_sql("lifeservice_shopids", engine_poi)["shopid"])
#     gotten_ids = set(pd.read_sql("lifeservice_latest",engine_poi)["shopid"])
#     remained_id = total_ids - gotten_ids
#     n1 = len(remained_id)
#     insert_newids()
#     n2 = len(remained_id)
#     if len(remained_id)>0:
#         insert_food(remained_id)
#         engine_poi.execute("SET SQL_SAFE_UPDATES = 0;")
#         clean_tb("lifeservice_history")
#         get_latest("lifeservice")
#         if n2>n1:
#             engine_poi.execute("INSERT update_logs VALUES(\"lifeservice\",%d,\"%s\");"%(n2-n1,time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))))
#         print("已经完成一轮更新！")
#     else:
#         print("暂时没有需要更新的！")
#     print("请等待下一轮更新！")
#     time.sleep(60*60*3)
#
#

# dl = pd.read_sql("proxys", engine_proxy)
# total_ids = set(pd.read_sql("lifeservice_shopids", engine_poi)["shopid"])
# remained_id = total_ids
# remained_id = list(remained_id)
# insert_food(remained_id)

while True:
    dl = pd.read_sql("proxys", engine_proxy)
    total_ids = set(pd.read_sql("lifeservice_shopids", engine_poi)["shopid"])
    gotten_ids = set(pd.read_sql("lifeservice_history", engine_poi)["shopid"])
    remained_id = total_ids - gotten_ids
    remained_id = list(remained_id)
    if len(remained_id)>0:
        insert_food(remained_id)
    else:
        print("已经爬完了！")
        time.sleep(60*60*24*15)