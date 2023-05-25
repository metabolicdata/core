---
description: This has been deprecated in 0.15.0+ in favour of sink ops
---

# 🦴 Sink Write Modes



When writing your output you are most likely working with partitions.

eventDtColumn allows you to specify where you want to partition by date (yyyy/mm/dd) addPartitionCol allows you to specify other partition columns by value

On top of this you might want to append incoming data or override it (recreating the historic for example), the combination of those two elements produce different results:

| writeMode | withPartition | Meaning                                                        |
| --------- | ------------- | -------------------------------------------------------------- |
| append    | no            | Adds resulting data as new file.                               |
| append    | yes           | Adds resulting data into existing and new partitions.          |
| overwrite | no            | Recreates the whole table.                                     |
| overwrite | yes           | Recreates only the partitions where resulting data is a match. |

####

\
\
