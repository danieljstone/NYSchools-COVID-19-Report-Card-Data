#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

As of October 8, 2007, New York State is not releasing state-wide school reopening data in one place.  Instead, you have to navigate to a portal and look up data about specific schools one by one.  This script attempts to remedy this by scraping and compiling school reopening data for New York State.  My next goals are to cross-reference this with demographic data and try to convert daily scrapes into a time series.

"""
import pandas as pd
from pandas.io.json import json_normalize
import requests
import json
import time 


def publicschoolinfo(schoolbeds,districtbeds):
    url="https://schoolcovidreportcard.health.ny.gov/data/public/school."+str(districtbeds)+"."+str(schoolbeds)+".json"
    response = json.loads(requests.get(url).text)
    return json_normalize(response)

def charterschoolinfo(beds):
    url="https://schoolcovidreportcard.health.ny.gov/data/charter/school.charter."+str(beds)+".json"
    response = json.loads(requests.get(url).text)
    return json_normalize(response)


def privateschoolinfo(beds):
    url="https://schoolcovidreportcard.health.ny.gov/data/private/school.private."+str(beds)+".json"
    response = json.loads(requests.get(url).text)
    return json_normalize(response)


def loadjson(url):
    response = json.loads(requests.get(url).text)
    return json_normalize(response)



privateschools=pd.read_json("https://schoolcovidreportcard.health.ny.gov/data/directory/private.directory.abbreviated.json").transpose().reset_index(drop=False)
publicschools=pd.read_json("https://schoolcovidreportcard.health.ny.gov/data/directory/public.directory.abbreviated.json").transpose().reset_index(drop=False)
charterschools=pd.read_json("https://schoolcovidreportcard.health.ny.gov/data/directory/charter.directory.abbreviated.json").transpose().reset_index(drop=False)



#errors are only rendered for schools with no data

privateschoolsinfo=pd.DataFrame()
for x in privateschools.schoolBedsCode: 
    try:
        y=privateschoolinfo(x)
        privateschoolsinfo=privateschoolsinfo.append(y)
    except:
        pass
publicschoolsinfo=pd.DataFrame()
for beds,districtbedscode in zip(publicschools.schoolBedsCode, publicschools.districtBedsCode):
    try:
        z=publicschoolinfo(beds,districtbedscode)
        publicschoolsinfo=publicschoolsinfo.append(z)
    except:
        pass
charterschoolsinfo=pd.DataFrame()
for x in charterschools.schoolBedsCode: 
    try:
        y=charterschoolinfo(x)
        charterschoolsinfo=charterschoolsinfo.append(y)
    except:
        pass


allschools=pd.concat([publicschools,privateschools,charterschools]).reset_index()
allschoolsinfo=pd.concat([publicschoolsinfo,privateschoolsinfo,charterschoolsinfo])

allschoolsdata=schoolsdata=allschools.merge(allschoolsinfo,how="left",left_on="schoolBedsCode",right_on="bedsCode").drop(["bedsCode","level_0"],1)

timestr = time.strftime("%Y%m%d-%H%M%S")
allschoolsdata.to_csv("data/forwardnydata_"+timestr+".csv")

