# -*- coding: utf-8 -*-

# Given a set of tables whose source data are stored as Avro files in GCS, extracts, for each table:
# - distinct values for every column, together with counts and min/max insertion time
#   (where insertion datetime column = RECORD_INSERTION_TIME)
# - 10 most frequent values for each column (and type, in case a column contains data of multiple types)
# - total number of values and number of null values for each column and time period
#   (period = year-month, taken from RECORD_INSERTION_TIME)
# - rows having null or invalid RECORD_INSERTION_TIME.

from __future__ import absolute_import

import argparse
import datetime
import gcsfs
import logging
import pandas as pd

from past.builtins import unicode

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from configTA import RECORD_INSERTION_TIME, project_id, region, local_folder, subnetwork, gcs_bucket_data, gcs_bucket_out

MAX_DATETIME = pd.to_datetime("1900-01-01T00:00:00")
MIN_DATETIME = pd.to_datetime("2100-12-31T00:00:00")

filename_processed_files = 'processed-avro-files.txt'
  
class ParseRowDoFn(beam.DoFn): # to be provided as input to a beam.ParDo()
        
    OUTPUT_TAG_INVALID = 'INVALID_RECORD_INSERTION_TIME' # for side output
    
    # Possible data types
    NULL     = 'null'
    EMPTY    = 'empty'
    INT      = 'int'
    FLOAT    = 'float'
    UNICODE  = 'unicode'
    DATETIME = 'datetime'
    
    def process(self, row): # input: a single row in one of the Avro files.
                            # Each row is provided as a dictionary: {column name --> column value at the row}.
        # Records with invalid insertion time (column RECORD_INSERTION_TIME) are filtered away.
        # Null or invalid insertion times are returned as a secondary output (with tag = OUTPUT_TAG_INVALID).
        valid_row = True
        record_insertion_time = row.get(RECORD_INSERTION_TIME) # row.get(...) to access the value of a specific column
        try: # check if record insertion time is valid
            if record_insertion_time is None: # insertion time is null
                valid_row = False
                yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_INVALID, self.NULL)
            record_insertion_time = pd.to_datetime(record_insertion_time)
        except Exception: # insertion time is not a valid datetime
            valid_row = False
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_INVALID, record_insertion_time)
        
        if valid_row:
            
            for col in row.keys(): # loop over columns (each row is provided as a dictionary with keys = column names)
                
                # Infer type for column col basing on current row
                value = row[col]
                
                if value == '':
                    vtype = self.EMPTY
                elif value is None or value != value:
                    vtype = self.NULL
                else:
                    if type(value) == unicode: # try to infer value type
                        vtype = type(value).__name__                        
                        try:
                            float(value)
                            try:
                                int(value)
                                vtype = self.INT
                            except Exception:
                                vtype = self.FLOAT
                        except Exception:
                            try:
                                pd.to_datetime(value)
                                vtype = self.DATETIME
                            except Exception:
                                vtype = vtype
                    else:
                        vtype = type(value).__name__
                     
                value = unicode(value)
                vtype = unicode(vtype)
                 
                # return: column name, inferred column type (based on current record), column value at current record,
                #         record insertion time
                yield ((col, vtype, value), record_insertion_time)
                

def run(argv = None):
    
    """Main entry point; defines and runs the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_bucket',
                        dest     = 'input_bucket',
                        required = True,
                        help     = 'GCS bucket containing all input Avro files for current table')    
    parser.add_argument('--output_bucket',
                        dest     = 'output_bucket',
                        required = True,
                        help     = 'GCS bucket to write all results to')
    parser.add_argument('--last_analysis_bucket',
                        dest     = 'last_analysis_bucket',
                        required = False,
                        default  = None,
                        help     = 'GCS bucket containing results from last analysis (if any)')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True  # see https://beam.apache.org/releases/pydoc/2.7.0/_modules/apache_beam/io/gcp/pubsub_it_pipeline.html
    p = beam.Pipeline(options = pipeline_options)
    
    # Retrieve list of files to process.
    # If another analysis has already been run, only the new files are analyzed and the results are then merged
    # with those from the last analysis.
    files_to_process = ['gs://{}'.format(fpath) for fpath in fs.walk(known_args.input_bucket)]
    is_first_analysis = True
    if known_args.last_analysis_bucket is not None:
        # the filename_processed_files file contains the list of all files that have already been processed
        # together with the id of the job where they have been processed, e.g.:
        # gs://all-my-data/system1/table1/data00001.avro    job_id_00001
        # gs://all-my-data/system1/table1/data00002.avro    job_id_00001
        # gs://all-my-data/system1/table1/data00003.avro    job_id_00002
        with fs.open(known_args.last_analysis_bucket+filename_processed_files) as f:
            already_processed_files = [row.split('\t')[0] for row in f.read().split('\r\n')]
            files_to_process = list(set(files_to_process) - set(already_processed_files)) # rule out files that have already been processed
        is_first_analysis = False
        
    print("{} avro files to process".format(len(files_to_process)))
    
    # Read Avro files and parse rows to extract information of interest:
    # columns, values, inferred types, record insertion times
    processed_input = \
           (p
            | 'get_files_list'          >> beam.Create(files_to_process)    # create PCollection containing the names of all files to process
            | 'read_files'              >> beam.io.avroio.ReadAllFromAvro() # returns all rows in all files as dictionaries 
                                                                            # {<column name> --> <column value at the row>}
            | 'parse_and_classify_rows' >> beam.ParDo(ParseRowDoFn()).with_outputs() # applies method ParseRowDoFn to each row
            )
    
    valid_inputs  = processed_input[None] # main output
    invalid_times = processed_input[ParseRowDoFn.OUTPUT_TAG_INVALID] # secondary output: list of invalid insertion times
    
    
    #-------------------#
    # PIPELINE BRANCH 1 # - count distinct values with min/max insertion time; filter 10 most frequent values for each column
    #-------------------#
    # This performs the equivalent of:
    #       select
    #           col, vtype, value
    #           count(*),
    #           min(record_insertion_time), # first time when value has appeared in this column
    #           max(record_insertion_time)
    #       from
    #           {source data}
    #       group by
    #           col, vtype, value
    # Inspired by: https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/combiners.py
    class CountMinMaxCombineFn(beam.transforms.core.CombineFn):

        def create_accumulator(self):
            return (0, MIN_DATETIME, MAX_DATETIME)
        
        def add_input(self, accumulator, element):
            count, mindate, maxdate = accumulator
            return count + 1, min(mindate, element), max(maxdate, element)
      
        def add_inputs(self, accumulator, elements):
            count, mindate, maxdate = accumulator
            return count + len(list(elements)), min(mindate, min(list(elements))), max(maxdate, max(list(elements)))
        
        def merge_accumulators(self, accumulators):
            counts, mindates, maxdates = zip(*accumulators)
            return sum(counts), min(mindates), max(maxdates)
        
        def extract_output(self, accumulator):
            return accumulator

    # Input:  ((col, vtype, value), record_insertion_time)
    # CountMinMaxCombineFn is called for each (col, vtype, value) and operates on element = record_insertion_time.
    # Output: ((col, vtype, value), (count, min_record_insertion_time, max_record_insertion_time))
    # (the output is grouped by key = (col, vtype, value))
    # [! AVOID using beam.GroupByKey - it is unsustainably slow]
    distinct_values = (valid_inputs
                       | 'value_counts' >> beam.CombinePerKey(CountMinMaxCombineFn())
                       )
    
    if not is_first_analysis:
        # Load and parse valuecounts result from last analysis
        # (i.e., the list of all distinct values with number of occurrences and min/max insertion time)
        def parse_valuecounts(filerow):
            col, vtype, value, counts, min_record_insertion_time, max_record_insertion_time = filerow.split('\t') # unicode
            return ((col, vtype, value),
                    (int(counts), pd.to_datetime(min_record_insertion_time), pd.to_datetime(max_record_insertion_time)))
    
        last_analysis = (p
                         | 'read_last_valuecounts' >> beam.io.ReadFromText(file_pattern = known_args.last_analysis_bucket+'*valuecounts*')
                         | 'parse_valuecounts'     >> beam.Map(parse_valuecounts)
                         )
        # output: ((col, vtype, value), (counts_last, mindate_last, maxdate_last))
        
        # Merge distinct_values with last_analysis and get incremental valuecounts over the whole history
        def merge_valuecounts(x):
            # input: ((col, vtype, value), [[(counts, mindate, maxdate)], [(counts_last, mindate_last, maxdate_last)])
            curr_valuecounts, last_valuecounts = x[1]
            # Case 1: value was just in new data and does not appear in old data
            if len(last_valuecounts) == 0: # this is a new value
                return (x[0], curr_valuecounts[0])
            last_valuecounts = last_valuecounts[0]
            # Case 2: value was just in old data and does not appear in new data
            if len(curr_valuecounts) == 0: # old value that has not appeared again in the new data
                return (x[0], last_valuecounts)
            # Case 3: value was both in old and in new data --> must merge the two results
            curr_valuecounts = curr_valuecounts[0]
            return (x[0], (curr_valuecounts[0] + last_valuecounts[0], 
                           min(curr_valuecounts[1], last_valuecounts[1]), 
                           max(curr_valuecounts[2], last_valuecounts[2])))
            
        distinct_values = ((distinct_values, last_analysis)
                            | 'cogroup_valuecounts' >> beam.CoGroupByKey()         # output: ((col, vtype, value), [[(counts, mindate, maxdate)], [(counts_last, mindate_last, maxdate_last)])
                            | 'merge_valuecounts'   >> beam.Map(merge_valuecounts) # output: ((col, vtype, value), (counts + counts_last, min(mindate, mindate_last), max(maxdate, maxdate_last)))
                            )
        # keep name distinct_values for the final output, so that it has the exact same form as it would have if is_first_analysis = True

    # *** Save *ALL* value counts for all columns (needed for incremental analysis) 
    (distinct_values # see https://www.freeformatter.com/csv-escape.html for an explanation on how to escape " characters
     | 'format_valuecounts' >> beam.Map(lambda x : x[0][0]+'\t'+x[0][1]+'\t"'+unicode(x[0][2].replace('"', '""'))+'"\t'+ \
                                                 ('\t'.join([unicode(el) for el in x[1]])))
     | 'save_valuecounts'   >> beam.io.WriteToText(known_args.output_bucket+'valuecounts')
     )
    # Output file rows have the format:
    # col	vtype	value	counts	mindate	maxdate
    # (values are separated by \t)
    
    # *** Filter and save TOP 10 VALUES (together with counts and min / max insertion time)
    def remap_for_filter(x): # input: ((col, vtype, value), (counts, mintime, maxtime))
        ctv, values = x
        return (ctv[0], ctv[1]), (values[0], (ctv[2], values[1], values[2])) # <--- beam.combiners.Top.LargestPerKey requires arguments 
                                                                             # to be provided in this form (in order to filter largest
                                                                             # by counts):
                                                                             # ((col, vtype), (counts, (value, mindate, maxdate)))
    
    top_values = (distinct_values
                  | 'remap_for_filter'  >> beam.Map(remap_for_filter)
                  | 'filter_top_values' >> beam.combiners.Top.LargestPerKey(10)
                  )

    def format_top_values(x):
        # input format: ((col, vtype), [(counts1, (value1, mindate1, maxdate1)), (counts2, (value2, mindate2, maxdate2)), ...]))
        # list of top 10 values for each (col, vtype)
        col, vtype = x[0]
        header = col+'\t'+unicode(vtype)+'\t' # header is common to all rows
        result = ''
        for top_value in x[1]: # tv = top value
            counts, (value, mindate, maxdate) = top_value
            value = '"'+unicode(value.replace('"', '""'))+'"' # escape " characters
            result += header + value +'\t'+unicode(counts)+'\t'+unicode(mindate)+'\t'+unicode(maxdate)+'\r\n'
        result = result[:-2] # remove last \r\n
        return result
    # result looks like this (for one single input)
    # col	vtype	value	counts1	   mindate1    maxdate1
    # col	vtype	value	counts2	   mindate2    maxdate2
    # ...
    # col	vtype	value	counts10   mindate10   maxdate10
    
    (top_values
     | 'format_top_values' >> beam.Map(format_top_values)
     | 'save_top_values'   >> beam.io.WriteToText(known_args.output_bucket+'topvalues')
     )
    

    #-------------------#
    # PIPELINE BRANCH 2 # - count total and null values by period (= year/month in this case)
    #-------------------#

    # Performs the equivalent of:
    #   select
    #       col,
    #       year-month,
    #       count(*),
    #       sum(if(value is null or value = '', 1, 0))
    #   from
    #       {input data}
    #   group by
    #       col,
    #       year-month
    
    class CountSumCombineFn(beam.transforms.core.CombineFn):
        # Operates on pairs (key, value) and performs the equivalent of the above query.
        # In our case,
        # key   = (col, year-month)
        # value = 1 if value in ('null', 'empty') else 0
        def create_accumulator(self):
            return (0, 0) # accumulator = (elements count, null/empty elements count). This is its initial value
        
        def add_input(self, accumulator, element): # element = 1 if value in ('null', 'empty') else 0
            count, sum_  = accumulator
            return count + 1, sum_ + element
      
        def add_inputs(self, accumulator, elements): # elements is a list
            count, sum_  = accumulator
            return count + len(list(elements)), sum_ + sum(elements)
        
        def merge_accumulators(self, accumulators): # accumulators is a list
            counts, sums = zip(*accumulators)
            return sum(counts), sum(sums)
        
        def extract_output(self, accumulator):
            return accumulator
  
    def remap_for_periodcounts(x):
        # input:  ((col, vtype, value), record_insertion_time)
        # output: ((col, year-month of record_insertion_time), 1 if value in ('null', 'empty') else 0))
        return ((x[0][0], unicode(pd.to_datetime(x[1]).strftime("%Y-%m"))), \
                1 if x[0][1] in (ParseRowDoFn.NULL, ParseRowDoFn.EMPTY) else 0)
  
    pcounts = (valid_inputs
               | 'remap_for_periodcounts' >> beam.Map(remap_for_periodcounts)
               | 'get_counts_by_period'   >> beam.CombinePerKey(CountSumCombineFn())
               )
    # output: ((col, period), (#records in period, #null records in period))

    if not is_first_analysis:
        # Load and parse periodcounts result from last analysis
        def parse_periodcounts(row):
            col, period, counts, nulls = row.split('\t')
            return ((col, period), (int(counts), int(nulls)))
    
        last_analysis_pc = (p
                            | 'read_last_periodcounts' >> beam.io.ReadFromText(file_pattern = known_args.last_analysis_bucket+'*periodcounts*')
                            | 'parse_periodcounts'     >> beam.Map(parse_periodcounts)
                            )
        
        def merge_last_periodcounts(x):
            # input:  ((col, vtype, value), [[(counts, nulls)], [(counts_last, nulls_last)]])
            curr_periodcounts, last_periodcounts = x[1]
            # Case 1: value was just in old data and does not appear in new data
            if len(curr_periodcounts) == 0:
                return (x[0], last_periodcounts[0])
            # Case 2: value was just in new data and does not appear in old data
            curr_periodcounts = curr_periodcounts[0]
            if len(last_periodcounts) == 0:
                return (x[0], curr_periodcounts)
            # Case 3: value was in both new and old data
            last_periodcounts = last_periodcounts[0]
            return (x[0], (curr_periodcounts[0] + last_periodcounts[0], curr_periodcounts[1] + last_periodcounts[1]))
        # output: ((col, vtype, value), (counts + counts_last, nulls + nulls_last))
        
        pcounts = ((pcounts, last_analysis_pc)
                    | 'cogroup_periodcounts' >> beam.CoGroupByKey() # full outer join on key = (col, vtype, value)
                    | 'merge_periodcounts'   >> beam.Map(merge_last_periodcounts)
                    )

    # Format and write outputs for periodcounts
    # results are of the form: ((column name, period), (nulls, counts))
    def format_periodcounts(x):
        return ('\t'.join([unicode(el) for el in x[0]]))+'\t'+('\t'.join([unicode(el) for el in x[1]]))
  
    (pcounts 
     | 'format_periodcounts' >> beam.Map(format_periodcounts)
     | 'save_periodcounts'   >> beam.io.WriteToText(known_args.output_bucket+'periodcounts')
     )
    # Output .txt file has format
    # col   period   nulls   counts
    # (values are separated by \t)

    #-------------------#
    # PIPELINE BRANCH 3 # - count invalid record insertion times (by value)
    #-------------------#
    
    # invalid_times is a simple PCollection of values, e.g. [null, 'kjn12xu81yu', -999, null]
    (invalid_times
     | 'count_invalid_times'  >> beam.combiners.Count.PerElement() # acts on a PCollection; output: (value, #occurrences)
     | 'format_invalid_times' >> beam.Map(lambda x : x[0]+'\t'+unicode(x[1]))
     | 'save_invalid_times'   >> beam.io.WriteToText(known_args.output_bucket+'invalidtimes')
     )

    result = p.run()
    result.wait_until_finish()
    
    return files_to_process # returns filepaths of processed files
      

if __name__ == '__main__':
    
    logging.getLogger().setLevel(logging.INFO)
    
    fs = gcsfs.GCSFileSystem()
    
    tables = {} # build dictionary: (source system, table) ---> bucket containing table data (Avro files)
    source_systems = [bkt.split('/')[-2] for bkt in fs.ls(gcs_bucket_data)]
    for system in source_systems:
        data_folder = gcs_bucket_data+system+'/'
        for folder in fs.ls(data_folder):
            table_name = folder.split('/')[-2]
            tables[(system, table_name)] = data_folder+table_name+'/'
            
    print(tables.keys())
        
    for system_table in sorted(tables.keys())[:1]:
        
        print("\n\n\n*** LAUNCHING DATAFLOW JOB FOR TABLE: {}\n".format(system_table))
        
        system, table = system_table
        output_bucket = gcs_bucket_out+"{system}/{table}/".format(system = system,
                                                                  table  = table)
        last_analysis_bucket = sorted(fs.ls(output_bucket))
        last_analysis_bucket = 'gs://'+last_analysis_bucket[-1] if len(last_analysis_bucket) > 0 else None
        
        print("Last analysis: {}".format(last_analysis_bucket))
        
        # Sample job_id: table-analyzer-system1-table1-20191217-1617
        job_id = "table-analyzer-{system_table}-{date_time}"
        job_id = job_id.format(system_table = ('-'.join(system_table)).replace('_', ''),
                               date_time    = datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
        # Sample output_bucket: gs://all-my-jobs/system1/table1/table-analyzer-system1-table1-201912181332
        output_bucket += job_id+'/'
                             
        args = ['--runner'               , "DataflowRunner", #"DirectRunner", #
                '--job_name'             , job_id,
                '--input_bucket'         , tables[system_table]+'2019/10/', # FIXME
                '--output_bucket'        , output_bucket,
                '--temp_location'        , output_bucket+'tmp/',
                '--project'              , project_id,
                '--staging_location'     , output_bucket+'tmp/',
                '--region'               , region,
                '--requirements_file'    , local_folder+"requirements.txt",
                '--last_analysis_bucket' , last_analysis_bucket,
                '--num_workers'          , '5',
                '--subnetwork'           , subnetwork]
        
        print("Input arguments:")
        for i in range(len(args))[::2]:
            print("{}\t{}".format(args[i], args[i + 1]))
        filepaths = run(args)
        
        # Write filepaths inside {filename_processed_files}.txt
        outwrite = ''
        if last_analysis_bucket is not None: # no append mode available - read file contents and re-write them
            with fs.open(last_analysis_bucket+filename_processed_files) as f:
                outwrite = f.read()+'\r\n'
        # attach new filepaths
        outwrite += '\r\n'.join(['{}\t{}'.format(fpath, job_id) for fpath in filepaths])
        outwrite = unicode(outwrite)
        with fs.open(output_bucket+filename_processed_files, 'w') as f:
            f.write(outwrite)
      
