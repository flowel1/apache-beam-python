# Data Exploration with Apache Beam

This script implements a sample Apache Beam pipeline to extract key **aggregate information** and quality metrics (number of corrupted records, most frequent values for each field, number of record insertions by time period...) from **huge amounts of raw relational data**.

Being written in **Python**, it is ideal for Data Scientists who need to perform an initial data exploration in order to identify potential opportunities to develop Machine Learning models.

## Use cases

**Data lakes** are typically populated with large amounts of raw data ingested from many heterogeneous source systems. Understanding the data's content and quality can often prove to be challenging, especially if no unified data governance policy has been adopted in the organization up to that moment. Some fields may contain valuable information deserving further analysis, while others may be obsolete or no longer populated. Moreover, corrupted or erroneous records may be present in unpredictable quantities.

Understanding the data is crucial in order to define further processing steps like data normalization, re-mapping or transfer to a data warehouse where they can be analyzed, displayed in dashboards or fed into Machine Learning models.

In cases like this, a useful starting point is to write some **data exploration script** extracting quick aggregate metrics and basic table / field information like number of corrupted records, most frequent values for each column, number of record insertions by time period, etc.

For limited amounts of data, all these calculations can be executed very straightforwardly by uploading the data into a pandas dataframe and running standard methods like ```df['column_of_interest'].value_counts()```. However, in these types of projects, data can rapidly grow too big to fit in memory, even for a single table. This requires that we go beyond local in-memory calculation and resort to some **Big Data technology** running on multiple computers in **parallel**.

For Data Scientists with limited experience with Java / Scala and parallel computation frameworks like Apache Spark, a good solution can be to write an **Apache Beam** pipeline in Python and run it on a **managed paid service** like Google Dataflow. Using a managed service has the advantage of not having to worry about dev-ops issues like cluster provisioning and management. All the analyst has to do is write the pipeline using the methods provided by apache_beam, which is not different from using any other Python library, and then submit the script to Google Dataflow by calling a dedicated API. In this way, the script will be executed in parallel on machines in Google's cloud.

## Our example: analyzing Avro files in Google Cloud Storage with Google Dataflow
We consider a use case where raw data are exported from several source systems and ingested into **Google Cloud Storage** buckets as **Avro files**. We assume that the bucket hierarchy has the following structure:

![alt-text](https://github.com/flowel1/apache-beam-python/blob/master/pictures/bucket-hierarchy.png)

We assume moreover that each table contains a technical column, say RECORD_INSERTION_TIME, containing the datetime when the record was inserted into the source table.
For each table, we would like to:
- find out whether there are records with null or erroneous (i.e., non-datetime) values of RECORD_INSERTION_TIME and store these values in a dedicated file
- for each column, find the different data types in the column and the top 10 most frequent values by type with the corresponding minimum and maximum RECORD_INSERTION_TIME values
- for each column and each time period (say, year-month), count the amount of records and null records inserted. This is useful in order to detect cases when data for a given column have only started being ingested after a certain time, or when they have stopped being ingested after a certain time.

### Prerequisites
You need to have a project in Google Cloud with billing activated in order to run this script. You must also have IAM permissions to read from / write to the necessary Google Cloud Storage buckets and to submit jobs to Google Dataflow.

The code is written using the Apache Beam SDK for Python. CAUTION: at the moment, **Python 2.7** must be used, since Apache Beam's version for Python 3 is still unstable. We expect this to change soon, since Python 2 will reach its end of life by January 2020.

We advise that you use **Anaconda** to manage Python environments and **Spyder** as a development environment for your Python scripts.

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


### Pipeline definition and submission

The Python script containing the pipeline definition and execution call can be written locally using any development environment (like Spyder). In the execution call, you must specify whether you want the pipeline to be executed locally on your computer (option ```runner = DirectRunner```) or in Google Dataflow (option ```runner = DataflowRunner```).
Local execution only makes sense for testing and bug fixing and should be launched on just a small subset of the available data (perhaps one or two Avro files), since a personal laptop would most likely take ages to run the whole job or not make it at all - which is the reason why we borrow Google's resources instead.

NB. for simpler types of analysis, BigQuery can be used instead:
create a table with external reference (must be done programmatically)
run approx_top_count on each column.

Write your pipeline in a method called run
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(pipeline_args)
p = beam.Pipeline(options = pipeline_options) # this declares your pipeline

# write all your pipeline definition + auxiliary methods

result = p.run()
result.wait_until_finish()    
```

Pipeline steps are defined by writing something like
```python
intermediate_output = (p | 'step 1' >> some_method(some_arguments) # the definition of some_method must also be provided
			 | 'step 2' >> some_other_method(some_other_arguments))
final_output = (intermediate_output | 'step 3' >> yet_another_method(yet_other_arguments))
```
In order to store the computation's results somewhere, you could end your pipeline with
```python
(final_output | 'write results' >> method_that_writes_results_to_some_file())
```

Try to use meaningful names for the steps.

If you need non-default packages in your script, you can list them in a ```requirements.txt``` file and provide the local file path as an input argument ```--requirements_file```.

### Relevant pipeline operations

Given a subclass of ```beam.DoFn```, say ```MyDoFn```, ```beam.ParDo(MyDoFn())``` runs the method "process" defined in ```MyDoFn``` on all elements of the input collection. The computation is performed in parallel.
The method ```process``` should return outputs in the form of pairs (key, value). If you want to return a dynamically generated list of outputs, use ```yield``` instead of ```return```.
```process``` may also return multiple distinct PCollections through tagged outputs:

[PSEUDO-CODE]
```
class MyDoFn(beam.DoFn):

	OUTPUT_TAG_1 = 'tag 1'
	OUTPUT_TAG_2 = 'tag 2'

	def process(self, input_collection):
		if some_condition:
			yield default_output
		elif some_other_condition:
			yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_1, alternative_output_1)
		else:
			yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_2, alternative_output_2)

output_collection = (input_collection | 'apply MyDoFn' >> beam.ParDo(MyDoFn()).with_outputs())

default_outputs, alternative_outputs_1, alternative_outputs_2 = output_collection[None], output_collection[MyDoFn.TAG1], output_collection[MyDoFn.TAG2]

std_pipeline = (default_outputs | 'apply some method' >> ...)
branch_1 = (alternative_outputs_1 | 'apply some other method' >> ...)
branch_2 = (alternative_outputs_2 | 'apply some third method' >> ...)
```

Each branch of the pipeline proceeds independently.

```beam.ParDo``` is ok when elements in the input collection must be processed independently, but what if we need to make some "group by" operations? Elements in a collection are always of the form (key, value) and Apache Beam allows us to do the equivalent of a "group by key". The key should therefore be defined based on the aggregation we want to perform.
We can define custom aggregation functions by creating a subclass of ```beam.transforms.core.CombineFn```, say ```MyCombineFn```, and then calling ```beam.CombinePerKey(MyCombineFn())```.
For each key, the aggregation result is stored in a so-called "accumulator" variable, which is a tuple consisting of one or more elements (one for each aggregate function we define). The accumulator is initialized and then updated dynamically as soon as new elements arrive. We must define both the initialization and the updating methods.
Multiple accumulators are initialized and updated in parallel on separate chunks of the input data and then merged once their computation is complete, so we must also define methods to merge two or more accumulators.
```python
class MyCombineFn(beam.transforms.core.CombineFn):
    def create_accumulator(self):
        # must define the initial value for the accumulator and return it.
        # The ideal choice of initial value is usually quite natural: for example, one should use 0 
        # if the aggregate function is a count, or an upper bound (e.g. 1E+50) if it is a min. 
        # If multiple aggregate functions are calculated in the same accumulator, a tuple should be 
        # defined containing the initial values of all the functions.

    def add_input(self, accumulator, element):
        # must define how the accumulator components must be updated when ONE new element element
        # appears; must return the updated accumulator components

    def add_inputs(self, accumulator, elements):
        # must define how the accumulator components must be updated when A LIST OF new elements
        # is processed; must return the updated accumulator components (same as add_input but 
        # with multiple elements instead of only one)

    def merge_accumulators(self, accumulators):
        # must define how to merge multiple accumulators that have been calculated separately on 
        # distinct subsets of elements. In practice, these multiple accumulators will come from distinct 
        # parallel computations whose final results are eventually merged.
        # Must return the merged accumulator components

    def extract_output(self, accumulator):
        # must define which output must be returned when the computation of accumulator has ended.
        # Typically, it is trivially equal to "return accumulator"
```

```beam.Map```: similar to ```beam.ParDo```, but simpler: can take a lambda function as input. Ok for basic operations like, e.g., tranforming pipeline results to strings prior to writing them to an output .txt file.

```beam.combiners.Top.LargestPerKey(10)```
(key, (size, value)) --> extracts the top 10 elements with largest size

We use CombinePerKey instead of GroupByKey because of better performance.

## Incremental pipeline
The file {filename_processed_files}.txt contains the list of all Avro files that have already been processed. It is updated dynamically at every launch.
