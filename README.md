# Welcome to metabolic

In short: DBT for a streaming data mesh

## What's metabolic?

Metabolic is a data tool for software developers to easly create and mantain an interface on their Data Domain. It allows to iterate on the technological decisions of the product without comprimising the reliability of dependant data applications, similar to contract testing in asynchronous services. 

Safely add, modify or deprecate columns, tables or streams while mantaining service for downstream applications, such as BI dashboards, Analytics products and Machine Learning.

You can read more at https://docs.getmetabolic.io

## Running metabolic

To run a simple job in batch
````
./metabolic.sh run example.conf
````

To run reload all the history
````
./metabolic.sh run --full-refresh example.conf
````

To run it in streaming
````
./metabolic.sh run --streaming example.conf
````
