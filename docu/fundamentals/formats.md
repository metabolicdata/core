---
description: Formats are the IO capabilities.
---

# ↔️ Formats

Formats make reading and writing trivial. Currently, the following physical formats are supported:

- Iceberg (default Table format)
- Delta Lake
- Parquet Files
- Json Files
- CSV Files
- Kafka Topics

Additionally Metabolic provides a virtual format named catalog.

## Batch vs Streaming formats

Metabolic doesn't restrict the use of a specific format whereas you want to run your entities in Batch or Streaming,
as other processing engines like Flink or KSQL do. Given said that, Iceberg, Delta and Kafka are preferred options if you
plan on switching a lot between them (for example following a Kappa Architecture) or you plan on having

## Iceberg

Iceberg is the default Metabolic Table format, as it provides atomic operations over a data lake, 
allowing file-based storages to behave like databases.

Iceberg operates by using a **catalog** for both reading and writing data, rather than relying directly on the underlying 
filesystem. This approach provides a more flexible and robust way to manage large-scale datasets, as the catalog stores 
metadata about the tables, including their schema, partitions, and versions.

[More on Iceberg](https://iceberg.apache.org/)

### How to read an Iceberg Source

```yaml
sources: [
  {
      catalog: ${dp.database}."my_table"
      name: data_lake_my_iceberg_table
      format: TABLE
      ops: [ ... ]
  }
  ...
]
```

### How to write an Iceberg Table

```yaml
sink: {
        catalog: ${dp.database_silver}."my_table_silver"
}
```

Iceberg supports **append**, **overwrite** and **upsert** write modes.

## Delta Lake

Delta Lake is another powerful table format supported by Metabolic, with automatic optimizations build in.

[More on Delta](https://docs.delta.io/latest/index.html)

### How to read a Delta Source

```yaml
sources: [
    {
        inputPath: ${dp.dl_clean_bucket}/my_input_table/
        name: data_lake_clean_my_delta_table 
        format: DELTA
        ops: [ ... ]
    }
    ...
]
```

### How to write a Delta Table

```yaml
sink: {
    outputPath: ${dp.dl_gold_bucket}/my_output_table
    format: DELTA
}
```

Delta supports ***append**, **overwrite**, **upsert** and **delete*** write modes. ***Upsert** and **delete*** need an *idColumn* param to identify
matching rows. Upsert optionally supports *eventDtColumn* to also match identical updates.

For example to upsert on event_id, but maintaining historical evolution (event source without duplicates):
```yaml
sink: {
    outputPath: ${ dp.dl_gold_bucket}/stream_events/
    writeMode: upsert
    idColumn: event_id
    eventDtColumn: event_created_at
    format: DELTA
    ops: [
      ...
    ]
}
```

## Parquet

Parquet is a very popular storage format ideal for data lakes, as it efficiently compresses columnar data for later
analysis in Hadoop compatible ecosystems.

[More on Parquet](https://parquet.apache.org/docs/overview/motivation/)

### How to read a Parquet Source

```yaml
sources: [
    {
        inputPath: ${dp.dl_clean_bucket}/my_input_table/
        name: data_lake_clean_my_parquet_table 
        format: PARQUET
        ops: [ ... ]
    }
    ...
]
```

### How to write a Parquet Table

```yaml
sink: {
    outputPath: ${dp.dl_gold_bucket}/my_output_table
    format: PARQUET
}
```


## Json

Json is another storage format popular in the analytics community, as it safely serializes records while maintaining a
very human-readable interface. Metabolic specifically uses the JSON Lines format.

[More on JSON](https://jsonlines.org/)

### How to read a JSON Source

```yaml
sources: [
    {
        inputPath: ${dp.dl_clean_bucket}/my_input_table/
        name: data_lake_clean_my_json_table 
        format: JSON
        ops: [ ... ]
    }
    ...
]
```

Since JSON doesn't force validation when being written, some types can be malformed. In this case you can use the
*useStringPrimitives* option to force to all columns as string type.

```yaml
sources: [
    {
        inputPath: ${dp.dl_clean_bucket}/my_input_table/
        name: data_lake_clean_my_json_string_table
        useStringPrimitives: true
        format: JSON
        ops: [ ... ]
    }
    ...
]
```

{% hint style="info" %}
Since *useStringPrimitives* does not discriminate columns, you can pair it with op.expr to convert other columns
back to the original type. 
{% endhint %}

### How to write a Json Table

```yaml
sink: {
    outputPath: ${dp.dl_gold_bucket}/my_output_table
    format: JSON
}
```

## CSV

CSV is another storage format popular in the analytics community, that stores records in a tabular way making it very
compatible with broader audience that use Microsoft Excel and alternates. Metabolic requires CSV files to provide a
header in the first line:

```
Year,Make,Model
1997,Ford,E350
2000,Mercury,Cougar
```

### How to read a CSV Source

```yaml
sources: [
    {
        inputPath: ${dp.dl_clean_bucket}/my_input_table/
        name: data_lake_clean_my_csv_table 
        format: CSV
        ops: [ ... ]
    }
    ...
]
```

### How to write a CSV Table

```yaml
sink: {
    outputPath: ${dp.dl_gold_bucket}/my_output_table
    format: CSV
}
```

{% hint style="info" %}
CSV sinks can be extremely useful at the end of entity transformations, as a deliverable for business stakeholders,
but it is not recommended as middle format when doing modular transformations.
{% endhint %}


## Kafka

Kafka topics are stream storages for events.

[More on Kafka](https://kafka.apache.org/documentation/#intro_concepts_and_terms)

### How to read a Kafka Source



```yaml
sources: [
    {
        topic: production.pub.raw.my_events
        name: data_lake_raw_kafka_events
        format: KAFKA
        ops: [ ... ]
    }
    ...
]
```

If your Kafka is protected by some authentication you can add the variable *kafkaSecret* to pass a resolver. Currently
Metabolic only support AWS SecretManager as a resolver, but in general you need a json with server, api key and secret.

```yaml
sources: [
    {
        format: KAFKA
        topic: "production.pub.raw.my_events"
        kafkaSecret: "production/kafka"
        name: "data_lake_raw_kafka_events"
        ops: [ ... ]
    }
    ...
]
```


### How to write a Kafka Topic

```yaml

sink: {
    format: KAFKA
    topic: production.pub.gold.my_events
    name: data_lake_gold_kafka_events
}
```

*name* is required in streaming jobs in order to create a checkpoint.