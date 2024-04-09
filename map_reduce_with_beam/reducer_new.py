#!/usr/bin/python

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(lines, seperator="\t"):
	for line in lines:
		# print(line.rstrip().split(seperator, 1))
		yield line.rstrip().split(seperator, 1)



if __name__ == "__main__":
	with open("output.txt", "r") as f:
		lines = f.readlines()

	data = read_mapper_output(lines, "\t")

	for current_word, group in groupby(data, itemgetter(0)):
		anagram_list = list(set(anagram for current_word, anagram in group))
		print(anagram_list)
		

