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

headers = '0MATERIAL, 0PLANT, 0STOR_LOC, ZMRP_ELEM, ZCIMCOUNT, ZPLANR, ZPLUMI, ZVRFKZ, ZDAT00, ZDELB00, ZMRP_EXTR, 0REQUIREMENT, 0ATPQTY, ZREVLV, ZVARSCRAP, 0P_PLANT, ZSTOR_LOC, ZPLAAB, 0CUSTOMER, 0VENDOR, ZTRAMG, 0BASE_UOM, ZZNUM08, ZUMDAT, ZAVAILQTY, 0GR_PR_TIME, 0DISMM, 0COMP_CODE, MSTAE, ZAUSSL, ZAUSKT, SELECTION_RULE, PLAN_SCENARIO, ZBERID, ZPAART, ZORIGCOMM, ZZCQRDT, ZFIXDTQTY, 0RECORDMODE, 0FISCVARNT, ZFISCWEEK, 0CALDAY, ZLDTFLAG, ZOGFISCWK, ZOGFISCPR, ZOGCALMON, ZOGCALWK, 0ISSVALSTCK, 0PREQU_VAL, 0DAY_NUM, ZOG_DAYS, MAT_PLANT, PLANT, ZOFFSET, OBJVERS, ZWLDTIME, ZWKSTART, BUDAT, periv, V_LDT, V_DATUM1, ZLDTFLAG, ZFISCWEEK1'

class CsvRecordSource(beam.io.filebasedsource.FileBasedSource):
  def __init__(
    self,
    file_pattern: str,
    delimiter: str = ",",
    quote_character: str = '"',
    compression_type: filesystem.CompressionTypes = filesystem.CompressionTypes.AUTO,
    include_filename: bool = False,
  ):
    self._splittable = False
    self.delimiter = delimiter
    self.quote_character = quote_character
    self.include_filename = include_filename
    super(CsvRecordSource, self).__init__(
      file_pattern,
      compression_type=compression_type,
      validate=True,
      splittable=self._splittable,
    )

  def read_records(self, filename: str, _offset_range_tracker):
    logging.debug("Reading CSV file %s", filename)
    with self.open_file(filename) as f:
      csv_reader = csv.reader(
        codecs.iterdecode(f, "utf-8"),
        delimiter=self.delimiter,
        quotechar=self.quote_character,

        )
      header = next(csv_reader)

      if header[0].startswith("\ufeff"):
        header[0] = header[0].encode("utf-8").decode("utf-8-sig")

      for row in csv_reader:
        data= dict(zip(header, row))
        if self.include_filename:
          data.update({"filename": filename})

        yield data
    

def date_validity_check(date: str) -> bool:
  if datetime.datetime.strptime(date, '%Y%m%d'):
    return True
  else:
    return False


def transform_columns(element: Dict[str, Any]) -> Dict[Any, Any]:
  col_list = [' ZDAT00', 'ZFISCWEEK', '0CALDAY']
  for c in col_list:
    if element[c]:
      if element[' ZORIGCOMM'] and element[' ZPLUMI'] == '-':
        if date_validity_check(element[' ZORIGCOMM']):
          element[c] = element[' ZORIGCOMM']
  return element
  

def calc_fiscal_week(element: Dict[str, Any]) -> Dict[Any, Any]:
  if element['BUDAT'] == element[' ZDAT00'] and element['periv'] == 'Z9':
    if element['ZOFFSET'] == "5000" and element['OBJVERS'] == 'A':
      element['ZFISCWEEK1'] = "5"
  else:
    element['ZFISCWEEK1'] = "6"
  if int(element['ZFISCWEEK']) < int(element['ZFISCWEEK1']):
    element['ZOGFISCWK'] = element['ZFISCWEEK1']
  else:
    element['ZOGFISCWK'] = element['ZFISCWEEK']
  return element


def calc_lead_time(element: Dict[str, Any]) -> Dict[Any, Any]:
  if element['MAT_PLANT'] == element['0MATERIAL'] and element['PLANT'] == element[' 0PLANT']:
    element['V_LDT'] = int(element['ZWLDTIME']) * 7

  if element['ZOFFSET'] == "5000" and element['OBJVERS'] == 'A':
    element['V_DATUM1'] = element['ZWKSTART']

  if not element['V_DATUM1']:
    element['V_DATUM1'] = element['V_DATUM1'] + element['V_LDT']

  if element[' ZDAT00'] < element['V_DATUM1']:
    element[' ZLDTFLAG'] = 'X'
  else:
    element[' ZLDTFLAG'] = ''
  return element


def calc_calendar_month(element: Dict[str, Any]) -> Dict[Any, Any]:
  element['ZOGCALMON'] = element[' ZDAT00'][0:6]
  return element


def get_week_from_date(element: Dict[str, Any]) -> Dict[Any, Any]:
  date_array = element[' ZDAT00'].split("/")
  element['ZOGCALWK'] = datetime.date(int(date_array[2]), int(date_array[0]), int(date_array[1])).isocalendar()[1]
  return element


def add_extra_mappings(element: Dict[str, Any]) -> Dict[Any, Any]:
  element["ZFISCWEEK"] = "4" #Adding constant value
  element["0CALDAY"] = None
  element["ZLDTFLAG"] = None
  element["ZOGFISCWK"] = None
  element["ZOGFISCPR"] = None
  element["ZOGCALMON"] = None
  element["ZOGCALWK"] = None
  element["0ISSVALSTCK"] = ""
  element["0PREQU_VAL"] = ""
  element["0DAY_NUM"] = ""
  element["ZOG_DAYS"] = ""
  # Adding additional columns with fake data as there is no information on the tables.
  element['MAT_PLANT'] = "10000LB"
  element['PLANT'] = "1920"
  element['ZOFFSET'] = "5000"
  element['OBJVERS'] = 'A'
  element['ZWLDTIME'] = "1000"
  element['ZWKSTART'] = '4'
  element['BUDAT'] = '5/25/2022'
  element['periv'] = 'Z9'
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


    orig_input = p  | 'Read' >> Read(CsvRecordSource(known_args.input))
  
    transform = (
      orig_input 
      | 'add_extra_mappings' >> beam.Map(add_extra_mappings)
      | 'transform_columns' >> beam.Map(transform_columns)
      | 'calc_lead_time' >> beam.Map(calc_lead_time)
      | 'calc_fiscal_week' >> beam.Map(calc_fiscal_week)
      | 'calc_calendar_month' >> beam.Map(calc_calendar_month)
      | 'get_week_from_date' >> beam.Map(get_week_from_date)
    )


    transform | 'Write' >> WriteToText(known_args.output)



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()