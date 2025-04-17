
<div align="center">
  <h1>🫀Metabolic: dbt for Streaming Data Mesh</h1>
</div>
<p align="center">
  <a href="https://github.com/metabolicdata/core/actions/"><img src="https://github.com/metabolicdata/core/actions/workflows/deploy.yml/badge.svg?branch=main" alt="Github Actions Badge"></a>
  <a href="https://github.com/metabolicdata/core/releases/"><img src="https://img.shields.io/github/v/release/metabolicdata/core?color=brightgreen&display_name=tag&logo=git&logoColor=white" alt="Latest Release"></a>
</p>

## What's metabolic?

**Metabolic** is a data tool for software developers to easily create and maintain an interface on top of their **Data Domain**. It enables iteration on technological decisions without compromising the reliability of dependent data applications — similar in spirit to **contract testing** for asynchronous services.

With Metabolic, you can **safely add, modify, or deprecate columns, tables, and streams** while maintaining service continuity for downstream consumers like BI dashboards, analytics tools, and machine learning workflows.

📚 Learn more at [docs.getmetabolic.io](https://docs.getmetabolic.io)

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
Want to help build Metabolic? Whether it's reporting issues, improving documentation, fixing bugs, writing tests, or contributing new features — we welcome your support!

👉 Check out the [contribution guide](CONTRIBUTE.md) for more info.