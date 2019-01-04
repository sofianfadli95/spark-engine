# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 11:23:52 2018

@author: CS
"""
city = []
loc_dict = {}

with open("Location2.txt", 'r', encoding="utf-8", errors = 'ignore') as f:
    for line in f:
        items = line.split("\t")
        key, values1, values2 = items[0], items[1], items[2]
        loc_dict[key.lower()] = { "lattitude" : values1.replace("\n",""), "longitude" : values2.replace("\n","") }
        city.append(key)
    city = set(city)

print(loc_dict["pulau dolak"])