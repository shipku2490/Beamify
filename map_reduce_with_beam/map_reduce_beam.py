import argparse
import logging
import re
import csv
import codecs
import json
import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText, filesystem, Read
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from typing import Dict, List, Any


en_stops = set(stopwords.words('english'))

def remove_stop_words(element):
  for words in element:
    for word in words:
      if len(word) > 0 and word not in en_stops:
          return ''.join(sorted(word)), separator, word

def spit_lines(file):
    line = re.split('[^a-z]+', line.lower())
    yield line


def read_mapper_output(element, separator='\t'):
    for line in element:
      yield line.rstrip().split(separator, 1)

def reduce_and_output_anagrams(element):
  for current_word, group in groupby(element, itemgetter(0)):
        anagram_list = list(set(anagram for current_word, anagram in group))
        if len(anagram_list) > 2:
            print "%s\t%s" % (len(anagram_list), anagram_list)


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  parser.add_argument(
      '--side_input_1',
      dest='sideinput1',
      required=True,
      help=' Side Input file to process..')
  parser.add_argument(
      '--side_input_2',
      dest='sideinput2',
      required=True,
      help='Side Input file to process..')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=pipeline_options) as p:

    lines = p | ReadFromText(known_args.input)

    # Count the occurrences of each word.
    counts = (
        lines
        | 'SplitLines' >> beam.FlatMap(spit_lines)
        | 'RemoveStopWordsAndMap' >> beam.FlatMap(remove_stop_words)
        | 'ReadMapperOutput' >> beam.Map(read_mapper_output)
        | 'ReduceAndOutputAnagram' >> beam.Map(reduce_and_output_anagrams)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write' >> WriteToText(known_args.output)



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()