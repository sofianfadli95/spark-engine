# -*- coding: utf-8 -*-
"""
Created on Sun Apr 15 22:07:34 2018

@author: CS
"""
example_dict = {"a" : 1}
try:
    f = open('log_error.txt', 'a')
    f.write(str(example_dict['b']))
except KeyError, e:
    f.write('I got an KeyError - reason "%s"' % str(e.message))
except IndexError, e:
    print 'I got an IndexError - reason "%s"' % str(e.message)