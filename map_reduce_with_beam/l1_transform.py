import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from typing import Dict, List, Any


FIELD_RENAMES = {
  "ZZMATNR": "0MATERIAL",
  "ZZWERKS": "0PLANT",
  "ZZLGORT": "0STOR_LOC",
  "ZZLIFNR": "0CUSTOMER",
  "ZZKUNNR": "0VENDOR",
  "ZZDISMM": "0DISMM",
  "ZZPLAAB": "ZPLAAB",
  "ZZDELKZ": "ZMRP_ELEM",
  "ZZCOUNTER": "ZCIMCOUNT",
  "ZZPLANR": "ZPLANR",
  "ZZPLUMI": "ZPLUMI",
  "ZZVRFKZ": "ZVRFKZ",
  "ZZDAT00": "ZDAT00",
  "ZZDELB00": "ZDELB00",
  "ZZEXTRA": "ZMRP_EXTR",
  "ZZMNG01": "0REQUIREMENT",
  "ZZMEINS": "0BASE_UOM",
  "ZZMNG04": "0ATPQTY",
  "ZZREVLV": "ZREVLV",
  "ZZMNG02": "ZVARSCRAP"
  }

class RenameFields(beam.PTransform):
  """Parse each line of input text into words."""
  def __init__(self, rename_dict, **kwargs):
    super(RenameFields, self).__init__(**kwargs)
    self.rename_dict = rename_dict

  @staticmethod
  def _process_element(element: Dict[Any, Any],rename_dict) -> Dict[Any, Any]:
    new_element = {}
    for k in list(element):
      if k in rename_dict:
        new_element[rename_dict[k]] = element.pop(k)
      else:
        new_element[k] = element.pop(k)

    return new_element

  def expand(self, input_or_inputs):
    return input_or_inputs | beam.Map(
      self._process_element, self.rename_dict

      )
    

def add_extra_mappings(element):
  element["0RECORDMODE"] = 1 #Adding constant value
  element["0FISCVARNT"] = "Z9"
  return element



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
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> ReadFromText(known_args.input)

    transform = (
        lines
        | 'add_extra_mappings' >> beam.ParDo(add_extra_mappings)
        | 'rename_columns' >> RenameFields(FIELD_RENAMES)
)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    transform | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()