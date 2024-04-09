#!/usr/bin/python

'''
Find anagrams in a text file
'''

import sys
import re
from nltk.corpus import stopwords


# list of stop words
en_stops = set(stopwords.words('english'))

file = "9198.txt"


def read_input(file):
    for line in file:
        line = re.split('[^a-z]+', line.lower())
        yield line


if __name__ == "__main__":
    with open(file, 'r') as f:
        lines = f.readlines()
       
    for words in read_input(lines):
        for word in words:
            if len(word) > 0 and word not in en_stops:
                print(''.join(sorted(word)), "\t", word)
    