from elasticsearch import Elasticsearch
from kafka import KafkaConsumer

import re
import sys
import json

print_keys = set()
consumer = KafkaConsumer('arcsight',
                         group_id='cef_consumer_group',
                         bootstrap_servers=['kafka1:9092'])




cefRegexHeader = re.compile(r'(?<!\\)(\S+?)\|')
cefRegexExtensions = re.compile(r'(\S+)(?<!\\)=')

for message in consumer:
    
    #print(str(message.value, 'utf-8'))
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
        elif counter ==  5:
            parsed['name'] =  matchHeader.group(1)
        elif counter == 6:
            parsed['severity'] =  matchHeader.group(1)
        counter += 1
    cefExtension = matchHeader.end()
    
    
    
    # Remainder of message contains extension key, values
    Extension = str(message.value, 'utf-8')[cefExtension:].lstrip()
    #print(extension)
    # use the token list from the Extension as the method of getting data up to but not including the next key. 
    tokenlist = "|".join(set(re.findall(cefRegexExtensions, Extension)))
    extensionKeys = re.compile('('+tokenlist+')=(.*?)\s(?:'+tokenlist+'|$)')
    
    
    continue_parsing = True
    while continue_parsing:
        m = re.search(extensionKeys, Extension)
        try:
            k,v = m.groups() 
            parsed[k] = v
            Extension = Extension.replace(k+'='+v, '').lstrip()
        except AttributeError:
            continue_parsing = False

    o = {}
    if len(print_keys) > 0:
        for p in print_keys:
            o[p] = parsed[p]
    else:
        o = parsed
    print(json.dumps(o))    
    
    
        