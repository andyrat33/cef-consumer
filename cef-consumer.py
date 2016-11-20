
from kafka import KafkaConsumer
from cef_keys import cef_keys
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
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          #message.offset, message.key,
                                          #message.value))
    print(str(message.value, 'utf-8'))
    parsed = {}
    
    
    #tokens = re.split(r'(?<!\\)\|', str(message.value, 'utf-8'))
    counter = 0
    cefExtension = 0
    for matchHeader in re.finditer(cefRegexHeader, str(message.value, 'utf-8')):
        if counter == 0:
            parsed['CEFVersion'] = matchHeader.group(1).split('CEF:')[1]
        elif counter == 1:
            parsed['DeviceVendor'] = matchHeader.group(1)
        elif counter == 2:
            parsed['DeviceProduct'] = matchHeader.group(1)
        elif counter == 3:
            parsed['DeviceVersion'] = matchHeader.group(1)
        elif counter == 4:
            parsed['DeviceEventClassId'] = matchHeader.group(1)
        elif counter ==  5:
            parsed['Name'] =  matchHeader.group(1)
        elif counter == 6:
            parsed['Severity'] =  matchHeader.group(1)
        counter += 1
    cefExtension = matchHeader.end()
    
    
    
    #print(parsed)
    Extension = str(message.value, 'utf-8')[cefExtension:].lstrip()
    #print(extension)
    # use redis to cache resuls of token set creation 
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
    
    #for cef in str(message.value, 'utf-8').split('|'):
        #print(" Value {!s}".format(cef))
        