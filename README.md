# Apache Spark
***
### Apache Spark Architecture

Spark is written in a language called scala, which runs on the Java Virtual Machine, or JVM. At first, in order to interact with spark, you had to write scala code, but now there are many different client libraries to interact with spark. This means you can harness spark's power using python, R, or a number of other langauges.

We will be using pyspark, the python interface to spark. Pyspark lets us write python code that manipulates spark dataframes (which, as we'll see, are similar to pandas dataframes), and the pyspark library will translate our python code into spark code that will run on the JVM.

Because pyspark is interfacing with spark, which is running on a JVM, we will sometimes see error messages and stack traces from Java when working with pyspark code.

***
## The Driver and Executors

Broadly speaking, a Spark application is broken into several areas:

1. The Driver: a single JVM (Java Virtual Machine) process, is responsible for analyzing your Spark program, optimizing your DataFrame queries, and determining how your Spark jobs should be parallelized.
1. The Cluster Manager: Gathers resources and schedules jobs on the cluster.
1. The Executors: which actually perform the distributed work.     


Each Spark program has a single Driver, a Cluster Manager, and one or more Executors. The Driver is usually your laptop, which will be running pyspark and will be connected to a cluster.

- The __Driver__ (on the laptop):

  - Runs pyspark
  - Analyzes and optimizes spark queries

- The __Master__, or __Cluster Manager__:

  - Allocates Resources
  - Creates Executors
  
- The __Executors__ on each node:

  - Read the data from various source(s)
  - Do the actual work
  
***

## Local Mode

Spark also can run on a single machine through what it calls local mode. In local mode, there are no Executor processes. Instead, the Driver also acts as (a single) Executor.

For this module, we will exclusively be running spark in local mode. We won't be getting into the finer points of operating or managing a spark cluster, rather, we will get familiar with the Spark API and writing pyspark code.

***
## Doing Work in Parallel

Spark parallelizes the work that it does, to the extent that it can. What this means is that multiple things are done at the same time, as opposed to doing one thing after another.

There are two levels to how work is parallelized in spark:

- All of the executors work together at the same time.
- Within each executor, the data is divided into partitions that can be processed at the same time. Generally speaking, the number of partitions is equal to the number of available CPU cores on the executor.
***
## Transformations and Actions

Spark dataframe manipulation can be broken down into two categories:

- __transformations__: A function that selects a subset of the data, transforms each value, changes the order of the records, or performs some sort of aggregation.
- __actions__: transformations that actually do something; something that necessitate that the specified transformations are applied. For example, counting the number of rows, or viewing the first 10 records.     

Often times, you will hear spark referred to as lazy. What this means is that we can specify many different transformations, but none of the transformations will be applied until we specify an action.
***

## Shuffling

A __shuffle__ occurs when a transformation requires looking at data that is in another partition, or another executor. Let's take a look at a few examples:

- Performing arithmetic on each number in a column does not require a shuffle as each number can be processed independently of the others.
- Sorting the dataframe by the numbers in a single column does require shuffling, as the overall order is determined by all of the data within all of the partitions.
- Selecting a subset of the data, for example, selecting only the rows where a condition matches, does not require a shuffle, as each row can be processed independently.
- Calculating the overall average for a numeric column does require shuffling, as the overall average depends on data from all the partitions.    

Shuffles get increasingly more expensive as the size of the data grows, and when a shuffle is performed is one of the largest considerations in optimizing spark code for performance.

***
## Environment Setup

#### Install Dependencies

Run the following commands from your terminal:
```
brew tap adoptopenjdk/openjdk
brew install adoptopenjdk11
```
After java is installed, we can install pyspark, the library that provides the python interface to spark:
```
python -m pip install pyspark
```
***
## Running Spark

In order to interact with spark, we'll create a spark object that lets us interface with the underlying spark instance and contains all of our enviornment configuration. In the rest of the lessons, we'll see more interaction with the spark object. This can be done in a python script, or a jupyter notebook.

Here is a simple example:
```
import pyspark

spark = pyspark.sql.SparkSession.builder.getOrCreate()
```
And here is a more complex example with more configuration options:
```
import multiprocessing
import pyspark

nprocs = multiprocessing.cpu_count()

spark = (pyspark.sql.SparkSession.builder
 .master('local')
 .config('spark.jars.packages', 'mysql:mysql-connector-java:8.0.16')
 .config('spark.driver.memory', '4G')
 .config('spark.driver.cores', nprocs)
 .config('spark.sql.shuffle.partitions', nprocs)
 .appName('MySparkApplication')
 .getOrCreate())
```
***
### Other Imports

It's common to import some functions from pyspark.sql.functions as well. You will see multiple functions from here as we progress through this module, and it is also common to see this module imported as F like this:
```
import pyspark.sql.functions as F
```
