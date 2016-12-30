# cef-consumer.py
import re
import os
import sys
import json
import time
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer

if sys.version_info[0] < 3:
    raise "Must be using Python 3"

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


def get_config():
    """ Return my config object. """
    conf = ConfigParser()
    conf.read('config/cef_consumer.cfg')
    return conf


config = get_config()

# config.get('cef_consumer', 'id')
# store the config in cef-consumer.ini

kafka = [server for server in config['kafka']['kafka'].split(',')]
# kafka settings
topic = config['kafka']['topic']
#kafka = ['kafka2:9092']
elasticsearch = dict()
elasticsearch['host'] = config.get('elasticsearch','host')
elasticsearch['port'] = config.get('elasticsearch','port')                     
#elasticsearch = {'host': 'es2016', 'port': 9200}

print_keys = set()

consumer = KafkaConsumer(topic, group_id='cef_consumer_group', bootstrap_servers=kafka)
es = Elasticsearch([elasticsearch])

cefRegexHeader = re.compile(r'(.*?)(?<!\\)\|')
cefRegexExtensions = re.compile(r'(\S+)(?<!\\)=')
i = 0
for message in consumer:
    i += 1

    # print(str(message.value, 'utf-8'))
    parsed = {}
    counter = 0
    cefExtension = 0
    for matchHeader in re.finditer(cefRegexHeader, str(message.value, 'utf-8')):
        if counter == 0:
            parsed['version'] = matchHeader.group(1).split('CEF:')[1]
        elif counter == 1:
            parsed['deviceVendor'] = matchHeader.group(1)
        elif counter == 2:
            parsed['deviceProduct'] = matchHeader.group(1)
        elif counter == 3:
            parsed['deviceVersion'] = matchHeader.group(1)
        elif counter == 4:
            parsed['deviceEventClassId'] = matchHeader.group(1)
        elif counter == 5:
            parsed['name'] = matchHeader.group(1)
        elif counter == 6:
            parsed['severity'] = matchHeader.group(1)
        counter += 1
    # noinspection PyRedeclaration
    cefExtension = matchHeader.end()

    # Remainder of message contains extension key, values
    Extension = str(message.value, 'utf-8')[cefExtension:].lstrip()
    # print(Extension)
    # use the token list from the Extension as the method of getting data up to but not including the next key.
    tokenlist = "|".join(set(re.findall(cefRegexExtensions, Extension)))
    extensionKeys = re.compile('(' + tokenlist + ')=(.*?)\s(?:' + tokenlist + '|$)')

    continue_parsing = True
    while continue_parsing:
        m = re.search(extensionKeys, Extension)
        try:
            k, v = m.groups()
            parsed[k] = v
            Extension = Extension.replace(k + '=' + v, '').lstrip()
        except AttributeError:
            continue_parsing = False

    parsed['cef_consumerId'] = os.environ.get("cef-consumerId", 'default')
    o = {}
    if len(print_keys) > 0:
        for p in print_keys:
            o[p] = parsed[p]
    else:
        o = parsed
    # print(json.dumps(o))
    # print a log line for docker logs
    print("{cef_consumerId} {logdate} {name} {catdt} {count}".format(**parsed, logdate=time.strftime('%d/%m/%Y %H:%M:%S', time.gmtime(int(parsed['rt']) / 1000.)), count=i))
    es.index(index=config.get('elasticsearch','index'), doc_type=config.get('elasticsearch','doc_type'), body=json.loads(json.dumps(o)))
