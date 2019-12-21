# Data Exploration with Apache Beam

This script implements a sample Apache Beam pipeline to extract key **aggregate information** and quality metrics (number of corrupted records, most frequent values for each field, number of record insertions by time period...) from **huge amounts of raw relational data** stored in a data lake.

Being written in **Python**, it is ideal for Data Scientists who need to perform an initial exploration in order to identify the most promising data.

## Use cases

Data lakes are typically populated with large amounts of raw data ingested from many heterogeneous source systems. Understanding the data's content and quality can often be challenging, especially if no unified data governance policy has been adopted in the organization up to that moment. Some fields may contain valuable information deserving further analysis, while others may be obsolete or no longer populated. Moreover, corrupted or erroneous records may be present in unpredictable quantities.

Understanding the data is crucial in order to define further processing steps like data normalization, re-mapping and transfer to a data warehouse where they can be analyzed, displayed in dashboards or fed into Machine Learning models.

For limited amounts of data, basic exploration can be done very straightforwardly by reading the data into a pandas dataframe and running standard methods like ```df['column_of_interest'].value_counts()```. However, in these types of projects, data can rapidly grow too big to fit in memory, even for a single table. This requires that we go beyond local in-memory calculation and resort to some **Big Data technology** running on multiple computers in **parallel**.

For Data Scientists with limited experience with Java / Scala and parallel computation frameworks like Apache Spark, a good solution is to write an **Apache Beam** pipeline in Python and run it on a **managed paid service** like Google Dataflow. Using a managed service has the advantage of not having to worry about dev-ops issues like cluster provisioning and management. All the analyst has to do is write the pipeline using the methods provided by the ```apache_beam``` library and then submit the pipeline for execution on Google Dataflow by calling a dedicated API. In this way, the pipeline will be run in parallel on multiple machines in Google's cloud.

## Our case: analyzing Avro files in Google Cloud Storage with Google Dataflow
We consider a use case where raw data are exported from several source systems and ingested into **Google Cloud Storage** buckets as **Avro files**. We assume that the bucket hierarchy has the following structure:

<img src="https://github.com/flowel1/apache-beam-python/blob/master/pictures/bucket-hierarchy.png" width="300">

We assume moreover that each source table has a technical column, say RECORD_INSERTION_TIME, containing the datetime when the record was inserted into the table.

Some of the questions we would like to answer are:

1) What types of data does each column contain? How many of these data are null or empty? This would help us discard useless columns (e.g. constant columns) and focus our attention on more interesting ones.

2) How recent are the non-null data in each column?

3) Are there any obsolete values in the columns (that have stopped being used after a certain moment in time to be replaced by some other value)?

Our pipeline helps addressing these questions by doing the following:
- for each column, find the different data types in the column (including nulls) and the top 10 most frequent values by type with the corresponding minimum and maximum RECORD_INSERTION_TIME values
- for each column and each time period (say, year-month), count the amount of records and null records inserted. This is useful in order to detect cases when data for a given column have only started being ingested after a certain time, or when they have stopped being ingested after a certain time.
- in order to avoid crashes, filter out the records with null or erroneous (i.e., non-datetime) values of RECORD_INSERTION_TIME and store these values in a dedicated file.

### Prerequisites
You need to have a project in Google Cloud with billing activated in order to run this script. You must also have IAM permissions to read from / write to the necessary Google Cloud Storage buckets and to submit jobs to Google Dataflow.

The code is written using the Apache Beam SDK for Python. At the moment, **Python 2.7** must be used, since the Apache Beam version for Python 3 is still unstable. We expect this to change soon, since Python 2 will reach its end of life by January 2020.

We recommend **Anaconda** to manage Python environments and **Spyder** as a development environment for your Python scripts.

To create a new virtual environment with Python 2.7 and activate it, you can open the Anaconda prompt and execute the following commands:
```
conda create -n py27 python=2.7
conda activate py27
```

The command to install the **Apache Beam SDK** is
```
pip install apache-beam
```
You will also need to install the gcsfs package to read from and write to Google Cloud Storage:
```
pip install gcsfs
```
Finally, installing the **Google Cloud SDK** will enable you to transfer files to Google Cloud Storage using the ```gsutil``` command in the command prompt.

If you have no prior experience with Apache Beam, we recommend reading the [Apache Beam Python SDK Quickstart](https://beam.apache.org/get-started/quickstart-py/]) and the related examples and documentation in order to familiarize with the basic concepts.


### Pipeline definition and submission

The Python script containing the pipeline definition and execution call can be written locally using any development environment (like Spyder). In the execution call, you must specify whether you want the pipeline to be executed locally on your computer (option ```runner = DirectRunner```) or in Google Dataflow (option ```runner = DataflowRunner```).
Local execution only makes sense for testing and bug fixing and should be launched on just a small subset of the available data (perhaps one or two Avro files), since a personal laptop would most likely take ages to run the whole job or not make it at all - which is the reason why we borrow Google's resources instead.

The pipeline definition is contained in a single script with the following structure:
```python
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run(args):
	known_args, pipeline_args = parser.parse_known_args(args)
	pipeline_options = PipelineOptions(pipeline_args)
	p = beam.Pipeline(options = pipeline_options) # this declares your pipeline

	# write all pipeline steps + related auxiliary methods
	# ...
	# ...

	result = p.run()
	result.wait_until_finish()
	
if __name__ == '__main__':
	args = ['--runner' 	      , "DataflowRunner", # "DirectRunner" to run locally
		'--requirements_file' , "local\\path\\to\\rqmts\\file" # if you need non-default packages in your pipeline
		# ...add all pipeline configuraion parameters here
		]
	run(args)
```

If you need non-default packages in your script (like gcsfs), you can list them in a ```requirements.txt``` file and provide the local file path as an input argument ```--requirements_file```.

Pipeline steps must be written in the following form (here we provide a toy example with token names):
```python
input_data = (p | 'read data' >> method_to_read_data())
intermediate_output = (input_data | 'processing step 1' >> some_method(some_arguments)
			 	  | 'processing step 2' >> some_other_method(some_other_arguments))
final_output = (intermediate_output | 'processing step 3' >> yet_another_method(yet_other_arguments))
```
In order to store the computation's results somewhere, you could end your pipeline with
```python
(final_output | 'write results' >> method_that_writes_results_to_some_file())
```
Branching pipelines may also be defined: for example, adding a few more lines like
```python
side_output = (intermediate_output | 'alternative processing' 	 >> alternative_processing_method()
				   | 'write alternative results' >> method_that_writes_results_to_some_file())
(final_output | 'write results to BigQuery too' >> method_that_writes_results_to_BigQuery())
```
generates a pipeline like this:

<img src="https://github.com/flowel1/apache-beam-python/blob/master/pictures/sample-pipeline.png" width="500">

Meaningful and unique names should be used for each of the pipeline steps.

### Relevant pipeline operations

All processing methods called in the various pipeline steps (```some_method```, ```some_other_method``` etc. in the toy example above) must be taken from standard classes in the ```apache_beam``` library, possibly extended or customized. They all take zero or more PCollections as input and return zero or more PCollections as output.

Our script uses the following Apache Beam classes to define the various processing steps. Here we just provide a quick overview; for further details, the official [Apache Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/) can be consulted.

- ```beam.Create```

Transforms a list of elements into a PCollection (raw lists cannot be processed by Apache Beam pipelines: only PCollections can).

- ```beam.io.avroio.ReadAllFromAvro```

Takes a PCollection of Avro file paths as input and returns the contents of all files in a single PCollection. Each element in the output PCollection contains data from one row in one of the input files, in the form of a dictionary {column name : column value at the row}.

- ```beam.io.ReadFromText(file_pattern)```

Reads the contents of the .txt file identified by path file_pattern and returns its contents into a PCollection. Each element in the PCollection is a string containing one row in the input file.

- ```beam.io.WriteToText(file_pattern)```

Takes a PCollection of strings as input and writes them all into the .txt file identified by path file_pattern.

- ```beam.Map(python_function)```

```python_function``` may be either a simple lambda function or a more complex function defined within a ```def my_function(x):``` code block. ```beam.Map``` applies the function ```python_function``` to all elements in the input PCollection and outputs the PCollection of transformed elements.

- ```beam.ParDo(MyDoFn())``` 

Similar to ```beam.Map``` (which can be though of as a special case of ```beam.ParDo```), but with a more complex structure that allows for multiple output PCollections to be returned instead of only one.

More in detail, given a custom subclass of the ```beam.DoFn``` class, say ```MyDoFn```, ```beam.ParDo(MyDoFn())``` runs the method "process" defined in ```MyDoFn``` on all elements of the input PCollection and returns the PCollection of transformed outputs. As the name ParDo suggests, the computation is performed in parallel.

```process``` may also return multiple distinct PCollections through the tagged outputs mechanism, which we illustrate in the toy example below:

```
class MyDoFn(beam.DoFn):

	OUTPUT_TAG_1 = 'tag 1'
	OUTPUT_TAG_2 = 'tag 2'

	def process(self, element_in_input_collection):
		if some_condition:
			yield default_transform(element_in_input_collection)
		elif some_other_condition:
			yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_1, alternative_transform_1(element_in_input_collection))
		else:
			yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_2, alternative_transform_2(element_in_input_collection))

output_collection = (input_collection | 'apply MyDoFn' >> beam.ParDo(MyDoFn()).with_outputs())

default_outputs, alternative_outputs_1, alternative_outputs_2 = output_collection[None], output_collection[MyDoFn.TAG1], output_collection[MyDoFn.TAG2] # in this case, beam.ParDo outputs three PCollections starting from a single PCollection

# go on with processing; apply a different logic to each of the three output PCollections
std_pipeline = (default_outputs | 'apply some method' >> ...)
branch_1 = (alternative_outputs_1 | 'apply some other method' >> ...)
branch_2 = (alternative_outputs_2 | 'apply some third method' >> ...)
```

After the output of _apply MyDoFn_ has been collected, the pipeline splits into three and each branch proceeds independently.

- ```beam.CombinePerKey(MyCombineFn())```

```beam.ParDo``` and ```beam.Map``` are ok when elements in the input PCollection must be processed independently, but what if we need to make some "group by" operations?

When elements in the input PCollection are tuples of the form (key, value), the equivalent of a "group by key" operation can be performed using the ```beam.CombinePerKey``` class. This implies that the key should be defined based on the aggregation we want to perform at a particular step.

Custom aggregation functions can be defined by creating a subclass of ```beam.transforms.core.CombineFn```, say ```MyCombineFn```, and then calling ```beam.CombinePerKey(MyCombineFn())``` in the pipeline step.

The aggregation result is updated dynamically as soon as new elements are processed. Its temporary value, which is constantly updated until it reaches its final value, is stored in a so-called "accumulator" variable, which is a tuple consisting of one or more elements (one for each aggregate function we define in the ```beam.transforms.core.CombineFn``` subclass).

In practice, multiple accumulators are initialized and updated in parallel on separate chunks of the input data and then merged once their computation is complete.

When writing our custom subclass of ```beam.transforms.core.CombineFn```, we must define the following methods:

```python
class MyCombineFn(beam.transforms.core.CombineFn):
    def create_accumulator(self):
        # must define the initial value for the accumulator and return it.
	# The accumulator is a tuple with one element for each aggregation we want to calculate.
        # The ideal choice of initial values is usually quite natural: for example, one should use 0 
        # if the aggregate function is a count, or an upper bound (e.g. 1E+50) if it is a min. 
        # If multiple aggregation functions are calculated in the same accumulator, a tuple should be 
        # defined containing the initial values for all the aggregation functions.

    def add_input(self, accumulator, element):
        # must define how the accumulator components must be updated when ONE new element
	# of the PCollection is passed to be processed; must return the updated accumulator components

    def add_inputs(self, accumulator, elements):
        # must define how the accumulator components must be updated when A LIST OF new elements
        # in the input PCollection is passed to be processed; must return the updated accumulator components
	# (same as add_input, but with multiple input elements instead of only one)

    def merge_accumulators(self, accumulators):
        # must define how to merge multiple accumulators that have been calculated separately on 
        # distinct subsets of elements. In practice, these multiple accumulators will come from distinct 
        # parallel computations whose final results must eventually be merged.
        # Must return the merged accumulator components

    def extract_output(self, accumulator):
        # must define which output must be returned when the computation of the accumulator has ended.
        # Typically, it is trivially equal to "return accumulator"
```

- ```beam.combiners.Top.LargestPerKey(n)```

Given an input PCollection with elements of the form (key, (size, value)), extracts the top n elements with largest size (n must be an integer number greater than 0).

- ```beam.CoGroupByKey```

Given two or more input PCollections with elements of the form (key, value), returns a PCollection whose elements are of the form (key, [list of values for elements in input PCollection having key = key]).

The syntax is
```python
((input_pcollection_1, input_pcollection_2, ..., input_pcollection_n) | 'cogroup' >> beam.CoGroupByKey())
```

- ```beam.combiners.Count.PerElement```

Given an input PCollection, returns a PCollection with elements (value, number of elements with value = value in input PCollection) for each distinct element value in the input PCollection.

### Pipeline structure
The file filename_processed_files contains the list of all Avro files that have already been processed. It is updated dynamically at every launch.

**Basic pipeline** (first launch)

![alt-text](https://github.com/flowel1/apache-beam-python/blob/master/pictures/pipeline-basic.png)

**Incremental pipeline** (successive launches)

![alt-text](https://github.com/flowel1/apache-beam-python/blob/master/pictures/pipeline-incremental.png)

### Monitoring Dataflow jobs
The job may take up to a few minutes to appear in the list of running jobs in the Dataflow section in your Google Cloud project. Clicking on the job name, the pipeline and all related information are displayed and updated dynamically as the job progresses.

Being a managed service, Dataflow automatically increases or decreases the number of working machines according to the amount of data being processed. This **autoscaling** process can be monitored in a time series plot:

![alt-text](https://github.com/flowel1/apache-beam-python/blob/master/pictures/autoscaling.png)

In the above plot, the target amount of workers could not be reached due to limitations set by the system administrator (adding more workers increases service costs).
