# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 17:05:29 2017

@author: dimas.herysantoso
"""

from configparser import ConfigParser
 
 
def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
 
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
 
    return db

consumer_key = 'gxX7XCSfkCq7G0OgKeN6CNvmd'
consumer_secret = 'p3ncFBTPbjLoHfkNPhkcuYgowCwyCeCA8Bobu58krlY5DUOcRH'
access_token = '954540830697467905-IllcTYlzdrevyj8g1DgC2taVg7rNtPW'
access_secret = 'aVmEE7d1P5p38RxTYDlDHW8zGCA73djwHbtzBLjSWO5Nu'


instag_client_id = "2307a6180bc74c0db8a5c90b832b7c63"
instag_client_sec = "886a2af22b4c4cdc8b7303d1f3aeaf15"
