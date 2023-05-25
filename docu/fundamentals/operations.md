---
description: >-
  An Operation (Op for short) is a recipe used by Data Specialists (like Data
  Engineers or Data Analysts), made available to other technical profiles.
---

# 💉 Operations

## Source Ops

### Filter Op

Restricts the input to a dynamic temporal window, based on a date time column. Work very well with [#defaults](placeholders.md#defaults "mention") placeholders.

```hoon
op.filter = {
    onColumn = "date_time"
    from = ${df.start_of_yesterday}
    to = ${df.now}
}
```

<details>

<summary>Filter Op Use Case: Incremental Loads</summary>

Most modern sources contain a way to rapidly identify new or updated values. For example adding an `updated_at` column in your table or storing inmutable events with an autoincremental `event_id` column.

This unlocks some business cases where you want to store those deltas to keep track of the history of that table or update a resulting insight with only new information.

Filter Op can transform the input source to a CTE that only contains new data from the period specify between `from` and `to`. For example you can filter between the 00:00:00.000 from yesterday to 00:00:00.000 of today to calculate a cummulative KPI values for the last 24 hours. And since it is deterministic you can run it multiples times and will produce the same result as long as the input data does not change.

</details>

### Prune Op

Restricts the input to a dynamic temporal window, based on a their temporal partitions. Work very well with [#defaults](placeholders.md#defaults "mention") placeholders.

```hoon
op.prune = {
    from = ${df.start_of_yesterday}
    to = ${df.now}
}
```

{% hint style="info" %}
You can use both op.prune + op.filter to increase efficiency of filtering on very large sources.
{% endhint %}

<details>

<summary>Prune Op Use Case: Filtering Json Files</summary>

In previous use case we've seen how filtering is a great technique to reduce the scope of some sources. But many times you don't have these columns you can use, or you have them but you would need to scan the full table.

Prune Op can transform the input source into a CTE that only contains data with matching partitions from the period specify between `from` and `to`.&#x20;

</details>

### Dedupe Op

**Removes** **duplicates** based on specified unique identifier **Id** criteria and by most recent **Order** criteria. This always returns one record per **id group, so no**&#x20;

```hoon
op.dedupe = {
    idColumns = ["id"]
    orderColumns = ["updated_at"]
    order = "asc"
}
```

| Parameter    | Description                                    | Required\|Default     |
| ------------ | ---------------------------------------------- | --------------------- |
| `idColumns`  | Colum names to deduplicate your rows on.       | Array with one value. |
| `order`      | Order in which the single record gets choosen. | `desc`                |
| orderColumns | Column names to order matching duplicates      | Empty array           |

This recipie is know as deduplication and allows to obtain the last unique state of a source.

<details>

<summary>Dedupe Op Use Case: Gold Layers</summary>

Most Data Lakes are a historical repository of data, meaning the inner layers of a datalake contain multiple versions of a curated entity. Sometimes though, its interesting to expose only the last version of that entity, specially in gold layers open to everyone to avoid confusion.\
\
Using dedupe on a curated `source` and just applying a `select * from deduped_source` mapping with a `replace` mode sink, will recreate the last version of that source everytime you run the job.

</details>

### Expression Op (since 0.10.0+)

**Adds** a new column based on a select expression. A mini mapping before the mapping or any other ops.

```hoon
op.expr = {
    expressions = ["timestamp(replace(updated, 'Z', '.000Z')) as updated_at"]
}
```

{% hint style="warning" %}
**Single** **expression** is also supported, but not recommended (and might be deprecated in future versions)

```
op.expr = {
    expression = "date(updated_at)"
}
```
{% endhint %}

<details>

<summary>Expression Op Use Case: Filtering Unformatted Data</summary>

For most transformations you don't want the whole data, but in order to use filter ops you need a properly iso formatted date time column, which might not be the case when processing raw data.\
\
Even though you could create a two step mapping, Expression Op empowers to apply this fast pre-transformation of the data in order to use it in another op, like filter, dedupe or demulti, so you end up with just one curated entity, while respecting&#x20;

</details>

### Drop Op (since 0.10.0+)

**Removes** an existing column. A mini mapping before the mapping or any other ops.

```hoon
op.expr = {
    columns = ["email", "phone"] 
}
```

{% hint style="warning" %}
**Single** **drop** is also supported, but not recommended (and might be deprecated in future versions)

```
drop = {
    column = "id"
}
```
{% endhint %}

<details>

<summary>Drop Op Use Case: Sensible Data</summary>

Certain entities might store sensible information. Fields like emails, ids, adresses, ips and many more are considered **Personal Indentificable Information** **(PII)** which is regulated in many countries.\
\
Several strategies are well known in the industry, like tagging the metadata to quickly identify this information or aggregate to reduce the granularity and make it more difficult to trace it back to certian individuals.\
\
But as developers, most of the time we have one more way: Simply remove them if they are not going to be used downstream: In metabolic you have two options to remove a column, either through a `drop op` or use a specific `select` statement to select only the desired columns.\
\
Since most of use use a `select *` , `drop op` is a very safe way to minimize exposing sensible fields.

</details>

### **Demulti Op**

**Generates** **duplicates** off a new specified time dimension (Day, Month or Year) named period.&#x20;

```hoon
op.demulti = {
    dateColumn = "updated_at"
    from = "${df.previous_first_day_of_year}"
    to = ${df.now}
    format = "Month"
    idColumns = ["id"]
    orderColumns = ["extracted_at"]
    endOfMonth = true
}
```



<table><thead><tr><th width="193.33333333333331">Parameter</th><th>Description</th><th>Required|Default</th></tr></thead><tbody><tr><td>dateColumn</td><td>Name of a timestamp column to generate the cohorts from</td><td>Required String</td></tr><tr><td>from</td><td>Timestamp of the first cohort.</td><td>Required (ISO)</td></tr><tr><td>to</td><td>Timestamp of the last cohort</td><td>Required (ISO)</td></tr><tr><td>format</td><td>Cohort period</td><td>"Day", "Month" or "Year"</td></tr><tr><td><code>idColumns</code></td><td>Colum names to deduplicate your rows on.</td><td>Array with one value.</td></tr><tr><td><code>orderColumns</code></td><td>Column names to order matching duplicates </td><td>Empty array</td></tr></tbody></table>

<details>

<summary>Demulti Op Use Case: Generating Monthly Status</summary>

WIP

</details>

### **Flatten Op**

Flattens structures into columns.

```hoon
op.flatten {
    column = "_airbyte_data"
}
```

If column is not specified it will flatten all the struct columns into **parent\_child** column names with the right format. If specified it will flatten only that parent column naming new columns directly with **child** names.

<details>

<summary>Flatten Use Case: Working with Airbyte Data</summary>

When using Airbyte with Datalakes (fileformat) , Airbytes uses structures to manage source data schema evolution. Usually creating one \_airbyte\_data field that requires quering \_airbyte\_data.property\_1 on all subsequent queries. This op will flatten this structure to obtain the original schema and queries will run natively.

</details>

## Mapping Operations

Mapping Operations have all the context of the sources and can be exectued before (preOps section) or after (postOps section) the SQL Mapping.

### Intervals Op (Beta)

**Generates** **intervalic** **states** off two sources with unrelated time dimensions.

```hoon
 op.intervals = {
     leftTableName = "clean_companies"
     rightTableName = "raw_subscriptions"
     leftIdColumnName = "id"
     rightIdColumnName = "id"
     leftWindowColumnName = "updated_at"
     rightWindowColumnName = "extracted_at"
     result: "companies_x_subscriptions"
}
```

<table><thead><tr><th>Parameter</th><th width="275">Description</th><th>Required|Default</th></tr></thead><tbody><tr><td>*TableName</td><td>name of source table</td><td>Required</td></tr><tr><td>*IdColumnName</td><td>name of joining column </td><td>Required</td></tr><tr><td>*WindowColumnName</td><td>name of a timestamp column </td><td>Required</td></tr><tr><td>result</td><td>name of resulting table</td><td>Required</td></tr></tbody></table>



This Operation produces a CTE with the two tables joined, yieliding a row for each state that happened in the past.

<details>

<summary>Intervals Op Use Case:  Joining Updates (Events)</summary>

Classic joins assume that either there's no temporal implications or that it exists a temporal window to match the records.\
\
This makes very hard to answer questions like: _What was the plan's price back to when this customer was subscribe to it_? Specially because both `plans` and `customers` are **mutable** **in time**, but they do **not share a specific window between timelines**. In summary: both plans and customers **updates** **are events**.\
\
You can use Interval Op to generate a single source of `customer subscription to plans` that contains all the states of the records that existed in the past. From there becomes so much trivial use your analytical SQL to obtain your desired insights.

</details>

## Sink Operations (0.15.0+)

\
There are two groups of sink operation, **transformative** and **operational:**

**Transformative** **Ops** modify the behaviour similarly to Source Ops:

### **Flatten Op**

See source [#flatten-op](operations.md#flatten-op "mention")for description an usage.

<details>

<summary>Flatten Use Case: Creating a bronze layer of Debezium Data.</summary>

When ingesting data into the datalake using debezium you will notice that you add extra information that is usally not very relevant for final users.\
\
Applying Flatten Op at Sink level allows to generate queryable layer of relevant fields (like payload.after) in a generic way, that helps build bronze layers (or the first actionable layer after the landing/raw layer)

</details>

**Operational** Ops do not modify the contents but the behaviour of how sinks are applied:

### Columns Partition Op

Defines which existing columns should be used for partitioning.

```hoon
op.cols_partition {
 cols = ["a","b","c"]
}
```

<details>

<summary>Columns Partition Use Case: Optimizing Dashboards Filters</summary>

Modern BI / Dashboarding tools that not preprocess data like Looker, Superset or Metabase rely on speed of the query engine to filter down the results. Having a partition on common filtered columns can heavily increase performance.

</details>

### Date Partition Op

Defines which date columns should be used for partitioning into date time components automatically.

```hoon
op.date_partition {
    col = "updated_at"
}
```

<details>

<summary>Date Partition User Case: </summary>



</details>

### &#x20;Schema Management Op

If you want metabolic to make all changes backwards compatible you can use manage\_schema op to automatically version your sinks.\


```hoon
op.manage_schema {}
```

