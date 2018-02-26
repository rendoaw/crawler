#!/usr/bin/env python

import urllib2
import json
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch,helpers
import traceback


def cleanup(txt):
    txt = txt.strip()
    txt = txt.replace('\t','')
    txt = txt.replace('\r','')
    txt = txt.replace('\n','')
    return txt


def get_page(url):
    ret = {}
    quote_page = url
    page = urllib2.urlopen(quote_page)
    soup = BeautifulSoup(page, 'html.parser')

    data = soup.find('div', attrs={'class':'well'})
    ret['description'] = data.text
    ret['url'] = url

    data = soup.findAll('h3', attrs={'class':'panel-title'})
    ret['title'] = cleanup(data[0].text)

    print ret['title']

    data = soup.findAll('table', attrs={'class':'table-vcenter'})

#    print data[0]
#    print
#    print data[1]
#    print
#    print data[2]
#    print
#    print data[3]
#    print
    rows = data[0].findAll('tr')
    info = {}
    for tr in rows:
        td = tr.findAll("td")
        key = cleanup(td[0].text)
        key = key.replace(' ','')
        value = cleanup(td[1].text)
        info[key] = value

    fee = {}
    if len(data) > 3:
        rows = data[2].findAll('tr')
        for tr in rows:
            td = tr.findAll("td")
            if len(td) == 0:
                continue
            key = cleanup(td[0].text)
            key = key.replace(' ','')
            value = ""
            if len(td) > 1:
                value = cleanup(td[1].text)
            fee[key] = value

    if len(data) > 3:
        rows = data[3].findAll('tr')
    else:
        rows = data[2].findAll('tr')
    schedule = []
    for tr in rows:
        td = tr.findAll("td")
        if len(td) < 5:
            continue
        dt = cleanup(td[0].text)
        day = cleanup(td[1].text)
        start = cleanup(td[2].text)
        end = cleanup(td[3].text)
        location = cleanup(td[4].text)
        entry = {}
        entry['date'] = dt
        entry['day'] = day
        entry['start'] = start
        entry['end'] = end
        entry['location'] = location
        schedule.append(entry)


    ret['info'] = info
    ret['fee'] = fee
    ret['schedule'] = schedule

    return ret



if __name__ == "__main__":
    actions = []
    es = Elasticsearch([{'host': '192.168.1.17', 'port': 9200}])

    for i in range(1,250):
        try:
            url = 'https://csasummercamps.recdesk.com/Community/Program/Detail?programId='+str(i)
            print url
            page_data = get_page(url)
            #print json.dumps(page_data, indent=4)

            action = {
                "_index": "bridgewater",
                "_type": "doc",
                "_id": page_data['url'],
                "_source": page_data
            }
            actions.append(action)
        except:
            print "Error: "+url
            print traceback.print_exc()

    es.indices.delete(index='bridgewater', ignore=[400, 404])
    es.indices.create(index='bridgewater')
    res = helpers.bulk(es, actions)
    print res
