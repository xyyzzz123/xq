# -*- coding: utf-8 -*-
# 正文、评论、用户，分开，用url链接起来
# 计算active window
# type = ???
import json
import sys
import yaml
import pickle
import requests
from collections import OrderedDict
import time
import re

import jieba
jieba.load_userdict("./userdict.txt")
from lib import fredis
from lib import blob


host = "10.172.136.41"
port = "30002"
redis_news_hkey = "snowball.newschema"
redis_comments_hkey = "snowball.commentschema"
redis_news_list = "snowball.updatenews"
redis_comments_list = "snowball.updatecomments"
blob_container = "snowballschema"


def get_article():
    r = fredis.Redis(host=host, port=port)
    b = blob.BlockBlob()

    i = r.hash.get(redis_news_hkey, "index")
    i = int(i["index"])

    try:
        while(True):
            vlist = r.queue.list(redis_news_list, i, i)
            if len(vlist) == 0: break
            print("Article " + str(i) + ": " + str(vlist[0]["url"]))

            json_schema = article_schema(vlist[0]["url"])
            blob_url = b.writeText("snowballschema", vlist[0]["blob"], json_schema)
            r.hash.insert(redis_news_hkey, {"k": vlist[0]["blob"], "v": blob_url})

            i += 1
    except:
        print("Interrupt at index " + str(i))
        r.hash.insert(redis_news_hkey, {"k": "index", "v": i})

    r.hash.insert(redis_news_hkey, {"k": "index", "v": i})



def article_schema(url):
    content = requests.get(url).content
    js = json.loads(content.decode("utf-8"))

    dict = OrderedDict()
    dict["id"] = js["id"]
    dict["user_id"] = js["user_id"]
    dict["title"] = js["title"]
    dict["url"] = "https://xueqiu.com" + js["target"]

    date = js["created_at"]
    dict["created_at"] = None if date == None else time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(date / 1000)))
    date = js["edited_at"]
    dict["edited_at"] = None if date == None else time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(date / 1000)))
    dict["posted_at"] = js["timeBefore"]

    dict["abstract"] = js["description"]
    dict["reply_count"] = js["reply_count"]
    dict["retweet_count"] = js["retweet_count"]
    dict["fav_count"] = js["fav_count"]
    dict["like_count"] = js["like_count"]

    dict["reward_count"] = js["reward_count"]
    dict["reward_amount"] = js["reward_amount"]
    dict["reward_user_count"] = js["reward_user_count"]

    dict["content"] = re.sub("<.*?>", "", js["text"])
    dict["related_codes"] = related_code(dict["content"])
    print(dict["related_codes"])

    schema = json.dumps(dict, ensure_ascii=False)
    print(str(schema) + "\n")
    # file = open("./schema.json", "w")
    # file.write(schema)
    # file.close()

    return schema



def related_code(text):
    code = set()
    with open("./dict.pk", 'rb') as f:
        dict = pickle.load(f)

    words = jieba.cut(text)
    for word in words:
        if word in dict.keys():
            code.add(dict[word])

    return list(code)


def company_dict():
    dict = {}
    with open("./sl.json") as f:
        for i, l in enumerate(f.readlines()):
            js = json.loads(l)
            print(str(i) + ": " + str(js))

            stock_name = js["stock_name"]
            company_name = js["company_name"]
            code = js["code"]

            dict[stock_name] = code
            dict[company_name] = code

    pickle.dump(dict, open("./dict.pk", "wb"))



def get_comments():
    r = fredis.Redis(host=host, port=port)
    b = blob.BlockBlob()

    i = r.hash.get(redis_comments_hkey, "index")
    i = int(i["index"])
    html = "https://financestore.blob.core.windows.net/xueqiu/"

    try:
        while (True):
            vlist = r.queue.list(redis_comments_list, i, i)
            if len(vlist) == 0: break

            yaml_str = html + str(vlist[0]) + ".yaml"
            print("Article " + str(i) + ": " + yaml_str)
            yaml_schema = comments_schema(yaml_str)
            blob_url = b.writeText(blob_container, str(vlist[0]) + ".yaml", yaml_schema)
            r.hash.insert(redis_comments_hkey, {"k": vlist[0], "v": blob_url})

            i += 1
    except:
        print("Interrupt at index " + str(i))
        r.hash.insert(redis_comments_hkey, {"k": "index", "v": i})

    r.hash.insert(redis_comments_hkey, {"k": "index", "v": i})


def comments_schema(url):
    clist = requests.get(url).content.decode("utf-8")
    clist = yaml.load_all(clist)

    comments = []
    for i, ym in enumerate(clist):
        dict = {}
        print("Comment " + str(i) + ": ", end="")

        if ym == None: continue
        dict["comment_id"] = ym["id"]
        dict["description"] = re.sub("<.*?>", "", ym["description"])
        dict["text"] = re.sub("<.*?>", "", ym["text"])

        date = ym["created_at"]
        dict["created_at"] = None if date == None else time.strftime("%Y-%m-%d %H:%M:%S",
                                                                     time.localtime(int(date / 1000)))
        dict["posted_at"] = ym["timeBefore"]

        dict["like_count"] = ym["like_count"]
        dict["reward_amount"] = ym["reward_amount"]
        dict["reward_count"] = ym["reward_count"]
        dict["reward_user_count"] = ym["reward_user_count"]
        dict["reply_comment_id"] = None if ym["reply_comment"] == None else ym["reply_comment"]["id"]
        dict["user_id"] = ym["user_id"]

        dict["user"] = {}
        dict["user"]["id"] = ym["user"]["id"]
        dict["user"]["screen_name"] = ym["user"]["screen_name"]
        dict["user"]["description"] = ym["user"]["description"]
        dict["user"]["followers"] = ym["user"]["followers_count"]
        dict["user"]["following"] = ym["user"]["friends_count"]
        dict["user"]["post_count"] = ym["user"]["status_count"]
        dict["user"]["url"] = "https://xueqiu.com/u" + ym["user"]["profile"]

        print(dict)
        comments.append(dict)

    print()
    comments_yaml = yaml.dump(comments, allow_unicode=True)

    return comments_yaml



if __name__ == '__main__':
    get_article()
    # get_comments()
    # company_dict()
    # seg_list = jieba.cut("药企创新力哪家强 阿斯利康新称王")  # 默认是精确模式
    # print("/".join(seg_list))