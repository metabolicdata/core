---
description: A Model is a tool to expose a data domain or part of it.
---

# 🫀 Models

A domain is exposed through one or more models and declared using a Config file.

```hoon
entities: [
    {
        name: My Domain Entity
        sources: [
            {
                catalog: data_lake.my_operational_table
                name: data_lake_my_operational_table_last_month
                op.filter: {
                   onColumn: period_month
                   from: ${df.start_of_month}
                   to: ${df.now}
                }
            }
        ]
        mapping: {
            sql: "SELECT period_month, count(distinct my_category) FROM data_lake_my_operational_table_last_month GROUP BY 1"
        }
        sink: {
            outputPath: ${dp.dl_gold_bucket}/my_analytical_table
            format: PARQUET
            writeMode: replace
            op.date_partition: {
                col: period_month
            }
        }
    }
]
```

Every Config file will produce at least one entity. Each entity is defined by four sections: A Name, Source(s), Mapping(s) and a Sink.

## Name

The human readable string that best defines the entity this model is producing. It is mostly used for logging and tracebility.

## Sources

In the Sources section you describe the entities in the Data Lake you are reading from. Each one is defined as:

1. An **input path**
2. A **name**
3. (Optional) A [**format**](placeholders.md#formats "mention"). By default it's Delta.
4. (Additionally) A **list of source [operations](placeholders.md#operations "mention").**

Before going into detail, it's important noting that everything in Metabolic is a CTE.

{% hint style="info" %}
A _CTE_ or _Common Table Expression_ are temporal results of a query that exists only within the context of a larger query. Much like a derived table, the result of a CTE is not stored and exists only for the duration of the query.
{% endhint %}

Metabolic will create a CTE from an input path and the specified format, and assign it a name. If operations are defined, it will apply them in order. This is what get exposed to the Mapping.

## Mappings

In the Mappings section you describe how to manipulate those sources into a final entity using SQL.

You can choose between inline SQL or through an external .sql file.

Because we generated all the sources as CTEs, the SQL syntax is standard, giving you extremely portability without sacrificing the extra power.

{% hint style="warning" %}
Be aware of the SQL flavours of advanced functions beyond SQL standard. Currently metabolic only support SparkSQL flavour.
{% endhint %}

Aditionally you can add a list of mapping operations.

## Sink

In the Sink section you describe how to materialize the output. It is defined as:

1. An output path
2. A write mode.
3. (Optional) A time partition key.
4. (Optional) A format. By default it's Delta.
5. (Additionally) A **list of operations**

## Overrides

When executing a model in historical mode, operations that constraint the temporal input or output are overiden.

