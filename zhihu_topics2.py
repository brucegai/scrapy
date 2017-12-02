import requests
from bs4 import BeautifulSoup
import re
import time
import pandas as pd
from random import choice,randint as ri
from sqlalchemy import create_engine
import gevent
import gevent.monkey
gevent.monkey.patch_socket()
engine_poi = engine_proxy = create_engine("mysql+pymysql://root:Audaque2016.com@172.16.1.209:3306/dianping_poi",echo=True,connect_args={'charset':'utf8'},pool_size=30)
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

def get_headers():
    headers = {
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding":"gzip, deflate, sdch, br",
        "Accept-Language":"zh-CN,zh;q=0.8",
        "Cache-Control":"max-age=0",
        "Connection":"keep-alive",
        "Cookie":"d_c0=\"AGDCXPMQCAuPThCn1z2_2ReWi5oqlORDB48=|1482299875\"; _zap=1495eb22-e265-43b2-b00a-5e7de4358836; q_c1=ab6912396b824b5ebc0a5aff6cf1c632|1485080584000|1482299875000; aliyungf_tc=AQAAANBqbSTFkwwAkYcOt1pJc5R9KQwo; _xsrf=4b4f758a381f1885ecdf995bf63b35d4; l_cap_id=\"ODllNWEwODc4ODczNGYyNjkwNzU2ZDc3NmQxZGZmMjk=|1487652676|2d8b48debd0706bd80bd199333a08708d44766df\"; cap_id=\"OGJmMDFjYzNmMjI5NGQyODk5ZGQ1NmJhM2UxMzdhYTc=|1487652676|ed1b84b8344ce8820039f26a5ac41d6e17b67500\"; n_c=1; __utma=51854390.1988585898.1487652749.1487652749.1487652749.1; __utmb=51854390.0.10.1487652749; __utmc=51854390; __utmz=51854390.1486630856.2.2.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmv=51854390.100--|2=registration_date=20141224=1^3=entry_date=20141224=1; z_c0=Mi4wQUFBQW5jcEZBQUFBWU1KYzh4QUlDeGNBQUFCaEFsVk5hbFRUV0FBZG51N0k4YjE1dTRxbnhFQmV0RTgzX0IycDl3|1487653841|4f69a6a36a71b21f1c36bf01b56bd27e024fa8e0; nweb_qa=heifetz",
        "Host":"www.zhihu.com",
        "Referer":"https://www.zhihu.com/topics",
        "Upgrade-Insecure-Requests":"1",
        "User-Agent":choice(USER_AGENTS)
    }
    return headers

def get_proxy(dl):
    n = choice(range(1, len(dl.index)))
    proxy = {"http":"http://%s:%s" %(dl["ip"][n],dl["port"][n])}
    return(proxy)

def get_childnodes(topic_id):
    url = "https://www.zhihu.com/topic/%s/hot"%topic_id
    resp = s.get(url,headers=get_headers(),proxies=get_proxy(dl))
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text,"lxml")
    topic = soup.select("h1")[0].text
    child_topic = [item.text.strip() for item in soup.select(".zm-side-section-inner.child-topic > .clearfix > a")]
    child_id = [item["href"].strip("/topic/") for item in soup.select(".zm-side-section-inner.child-topic > .clearfix > a")]
    try:
        attention_num = soup.select(".zm-topic-side-followers-info strong")[0].text
    except:
        attention_num = "NA"
    if len(child_topic)>1:
        df = pd.DataFrame(dict(topic=topic,attention_num=attention_num,topic_id=topic_id,child_topic=child_topic,child_id=child_id))
        df.to_sql(con=engine_poi,name="zhihu_topics",if_exists="append",flavor="mysql",index=False)
    elif len(child_topic)==1:
        df = pd.DataFrame(dict(topic=topic,attention_num=attention_num,topic_id=topic_id,child_topic=child_topic,child_id=child_id),index=[0])
        df.to_sql(con=engine_poi,name="zhihu_topics",if_exists="append",flavor="mysql",index=False)
    elif len(child_topic)==0:
        pass
    id_temp.append(child_id)


id_temp = []
id_list = list(pd.read_sql("last_id_list",engine_poi)["id"])
dl = pd.read_sql("proxys", engine_proxy)

while id_list:
    gotten_ids = set(pd.read_sql("zhihu_topics", engine_poi)["topic_id"])
    id_list = set(id_list)-gotten_ids
    for id in id_list:
        try:
            get_childnodes(id)
        except:
            try:
                get_childnodes(id)
            except:
                print("爬不了了！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！")
    id_list = sum(id_temp,[])
    id_tb = pd.DataFrame(dict(id=id_list))
    id_tb.to_sql(con=engine_poi,name="last_id_list",flavor="mysql",if_exists="replace",index=False)
    id_temp = []

# while len(id_list)>0:
#     for id in id_list:
#         try:
#             get_childnodes(id)
#         except:
#             try:
#                 get_childnodes(id)
#             except:
#                 pass
#     id_list = sum(id_temp,[])
#     id_temp = []