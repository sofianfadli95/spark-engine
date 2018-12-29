eeeeeee# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 14:03:12 2018

@author: sofyan.fadli
"""

"""

Codingan ini berisi fungsi yang digunakan untuk melakukan
data preprocessing...

"""

from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
import re

# Inisialisasi fungsi Stemmer bahasa Indonesia
# Stemmer untuk membuang semua imbuhan dan mendapatkan kata dasarnya
factory = StemmerFactory()
stemmer = factory.create_stemmer()

lookup_dict = {}
# Dictionary utk lemmatize
with open("formalizationDict.txt", 'r') as f:
    for line in f:
        items = line.split()
        key, values = items[0], items[1:]
        lookup_dict[key] = ' '.join(values)

# Fungsi untuk menimpa kata-kata yang salah / alay dengan kata
# yang terdapat pada formalizationDict
# Contoh : gpp => tidak apa-apa
#          egp => emang saya pikirin
def _lookup_words(input_text):
    words = input_text.split() 
    new_words = []
    new_text = ""
    for word in words:
        if word.lower() in lookup_dict:
            word = lookup_dict[word.lower()]
        new_words.append(word)
        new_text = " ".join(new_words) 
    return new_text

# Removes punctuation, parentheses, question marks, etc., and leaves only alphanumeric characters
strip_special_chars = re.compile("[^A-Za-z ]+")
def cleanSentences(string):
    string = string.lower().replace("<br />", " ")
    return re.sub(strip_special_chars, " ", string.lower())

