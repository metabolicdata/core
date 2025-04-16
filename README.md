
<h1 style="text-align: center;">🫀Metabolic: dbt for Streaming Data Mesh</h1>
<p align="center">
  <a href="https://github.com/metabolicdata/core/actions/"><img src="https://github.com/metabolicdata/core/actions/workflows/deploy.yml/badge.svg?branch=main" alt="Github Actions Badge"></a>
  <a href="https://github.com/metabolicdata/core/releases/"><img src="https://img.shields.io/github/v/release/metabolicdata/core?color=brightgreen&display_name=tag&logo=duckdb&logoColor=white" alt="Latest Release"></a>
</p>

## What's metabolic?

Metabolic is a data tool for software developers to easly create and mantain an interface on their Data Domain. It allows to iterate on the technological decisions of the product without comprimising the reliability of dependant data applications, similar to contract testing in asynchronous services. 

Safely add, modify or deprecate columns, tables or streams while mantaining service for downstream applications, such as BI dashboards, Analytics products and Machine Learning.

You can read more at https://docs.getmetabolic.io

## Running metabolic

To run a simple job in batch
````shell
./metabolic.sh run example.conf
````

To run reload all the history
````shell
./metabolic.sh run --full-refresh example.conf
````

To run it in streaming
````shell
./metabolic.sh run --streaming example.conf
````

## Contributing
If you want to help building Metabolic, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. See this [this document](CONTRIBUTE.md) for details.