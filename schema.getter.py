import json
import yaml
import pickle
import requests
from collections import OrderedDict
import time
import re
import jieba
jieba.load_userdict("./userdict.txt")

from lib.log import Log
from lib.fredis import Redis
from lib.blob import AppendBlob, BlockBlob

host = "10.172.136.41"
port = "30002"
redis_news_hkey = "snowball.newschema"
redis_comments_hkey = "snowball.commentschema"
redis_news_list = "snowball.updatenews"
redis_comments_list = "snowball.updatecomments"
blob_container = "snowballschema"
delay = 30 # in seconds

log = Log("stdout")