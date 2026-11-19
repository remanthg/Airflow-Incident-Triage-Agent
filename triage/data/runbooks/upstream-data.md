# Upstream data

## RB-101 | Missing input partition

A consumer can fail when an upstream producer has not published the required input partition for the task's data interval. Evidence includes a file or object lookup that returns NoSuchKey, a 404 for the expected parquet file, FileNotFoundError on the expected upstream input, or an empty required table partition that prevents the consumer from proceeding. Check the requested interval and path against the producer's recorded output.

## RB-102 | Missing completion marker

Some producers publish a _SUCCESS marker or commit manifest after finalizing a batch. When the marker or manifest is required, its absence after the publication window means the input is unavailable to the consumer even if the directory exists. A read-only check is to inspect the producer attempt and list its output and marker for the interval.
