# HiveDdbQueryUDTF

![main](https://github.com/klimber/HiveDdbQueryUDTF/actions/workflows/maven.yml/badge.svg?branch=main)

Welcome to HiveDdbQueryUDTF, an [Apache Hive](https://hive.apache.org/) user defined table function 
([UDTF](https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF)) that allows 
[querying](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) an
Amazon [DynamoDB](https://aws.amazon.com/pt/dynamodb/) table from Amazon EMR using SQL. 

## Overview

HiveDdbQueryUDTF allows you to fetch data from dynamoDB within an SQL statement using partition key
data that may be available as input. Some use cases include, but are not limited to: 

### Deduplication

Suppose you keep a historic table of all the records you process on DynamoDB. You also perform
data transformations on EMR on an incremental daily (or other period ranges) dataset. The daily 
dataset contains data that allows you to derive the DynamoDB partition key for the records in that
dataset. You can then use HiveDdbQueryUDTF to query DynamoDB and check whether any of the records 
in the daily dataset was already processed before.

### Fetch more data

When performing transformations on EMR, your dataset is missing some columns that you could fetch
from DynamoDB and you are able to derive the DynamoDB partition key. You could use HiveDdbQueryUDTF
to fetch the missing columns.

## How it works

Hive UDTFs apply their behavior for each record of input data, returning zero to several rows of
results for each record. This allows HiveDdbQueryUDTF to query DynamoDB once per input record and
fetch the required results.

## EMR Versions

* HiveDdbQueryUDTF 1.x.y was written to work with EMR release 5.32+
* EMR release 6+ is still not supported, but should be coming soon. 

## How to use

Suppose you have a table called `local_data`, containing a column named `entity_id`, which is used
as the partition key for records on DynamoDB. The table on DynamoDB is called `ddbData`, and it's partition
key attribute is named `pkAttribute`. You could use HiveDdbQueryUDTF to fetch the data like this.

First, the HiveDdbQueryUDTF jar should be available to your EMR cluster, either by using bootstrap to copy it to 
`usr/lib/hive/auxlib/` or calling `add jar` during SQL execution.

```sql
add jar s3://<your-bucket>/path/to/hiveddbudtf-x.y.z.jar;
```

Second, initialize HiveDdbQueryUdtf as a function, you can choose the function name

```sql
create temporary function ddb_query as 'com.klimber.hiveddbudtf.HiveDdbQueryUdtf';
```

Third, use it on queries!
```sql
select 
    ddb_query(
        named_struct(
            'tableName', 'ddbData',
            'indexName', null,
            'hiveDdbColumnMapping', 'my_column_1:myAttribute1,my_column_2:myAttribute2',
            'hiveTypeMapping', 'string,bigint'
        ),
        struct(
            named_struct(
                'attribute', 'pkAttribute',
                'attributeType', 'S',
                'operator', 'EQ',
                'value', local_data.entity_id
            )
        )
    )
from local_data;
```

The SQL above would query DynamoDB once for each row of `local_data`, retrieving the records
where `pkAttribute = local_data.entity_id`. The result of this SQL is a table containing two
columns: `my_column_1` and `my_column_2`, each containing the data for the attributes `myAttribute1`
and `myAttribute2`, respectively. You could write this result to a table by adding 
`create table ddb_data as` before the `select` keyword.

Another option is to use a `lateral view` to join the results from dynamoDB with the `local_data` table.
Which would result in a table containing all columns from `local_data`, followed by the columns from the
dynamoDB query.
```sql
select 
    local_data.*,
    ddb_data.*
from local_data
lateral view ddb_query(
    named_struct(
        'tableName', 'ddbData',
        'indexName', null,
        'hiveDdbColumnMapping', 'my_column_1:myAttribute1,my_column_2:myAttribute2',
        'hiveTypeMapping', 'string,bigint'
    ),
    struct(
        named_struct(
            'attribute', 'pkAttribute',
            'attributeType', 'S',
            'operator', 'EQ',
            'value', local_data.entity_id
        )
    )
) ddb_data;
```

The `LATERAL VIEW OUTER` option could be used to simulate a left join.

### Parameters

HiveDdbQueryUdtf requires 2 parameters, both structs.

#### First parameter: [HiveDdbQueryParameters](src/main/java/com/klimber/hiveddbudtf/hive/HiveDdbQueryParameters.java)

This parameter is responsible for defining the DynamoDB table to query, which index to use and how
the resulting table will look like. You can provide it by using the `named_struct` hive function, 
which works by receiving key-value pairs:
```sql
named_struct(
    'tableName', 'ddbData',
    'indexName', null,
    'hiveDdbColumnMapping', 'my_column_1:myAttribute1,my_column_2:myAttribute2',
    'hiveTypeMapping', 'string,bigint'
)
```

* **tableName** defines the DynamoDB table to query.
* **indexName** defines which DynamoDB index to use, must be passed as `null` if not using an index.
  When using an index, make sure to consider the index projection expression when defining column mappings.
* **hiveDdbColumnMapping** works just like `dynamodb.column.mapping` on [emr-dynamodb-connector](https://github.com/awslabs/emr-dynamodb-connector).
  Should be provided as pairs of strings separated by `:` between and `,` for each pair, where the first defines the resulting
  column name on hive, and the second defines the desired attribute name on DynamoDB for that hive column.
  Should **not** contain spaces.
* **hiveTypeMapping** defines, for each column mapping on `hiveDdbColumnMapping`, the resulting column type
  on hive. Should contain one column type for each mapping. Should **not** contain spaces.

Supported Hive / DynamoDB types, you should consider these when providing adequate values to `hiveTypeMapping`

| Hive type        | DynamoDB types                                              |
|------------------|-------------------------------------------------------------|
| string           | string (S)                                                  |
| bigint or double | number (N)                                                  |
| binary           | binary (B)                                                  |
| boolean          | boolean (BOOL)                                              |
| array            | list (L), number set (NS), string set (SS), binary set (BS) |
| map or struct    | map (M)                                                     |

Example error of setting type `bigint` when DynamoDB contains decimals:
```text
Caused by: java.lang.NumberFormatException: For input string: "1.11"
	at java.lang.NumberFormatException.forInputString(NumberFormatException.java:65)
	at java.lang.Long.parseLong(Long.java:589)
	at java.lang.Long.parseLong(Long.java:631)
	at org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser.getNumberObject(DynamoDBDataParser.java:242)
	at org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser.getNumberObjectList(DynamoDBDataParser.java:234)
	at org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBNumberSetType.getHiveData(HiveDynamoDBNumberSetType.java:52)
	at com.klimber.hiveddbudtf.HiveDdbQueryUdtf.toHiveData(HiveDdbQueryUdtf.java:131)
```

Example error of setting type `string` for a `binary` dynamoDB attribute. 
```text
Caused by: org.apache.hadoop.hive.ql.metadata.HiveException: Hive Runtime Error while processing row {"entity_id":"0123456789TESTCODE"}
	at org.apache.hadoop.hive.ql.exec.MapOperator.process(MapOperator.java:570)
	at org.apache.hadoop.hive.ql.exec.tez.MapRecordSource.processRow(MapRecordSource.java:92)
	... 18 more
Caused by: java.lang.IllegalArgumentException: Hive type 'string' does not support DynamoDB type 'B' (ddbAttributeName=fieldBinary)
	at com.klimber.hiveddbudtf.HiveDdbQueryUdtf.toHiveData(HiveDdbQueryUdtf.java:129)
	at com.klimber.hiveddbudtf.HiveDdbQueryUdtf.lambda$process$0(HiveDdbQueryUdtf.java:111)
	at java.util.stream.ReferencePipeline$3$1.accept(ReferencePipeline.java:193)
	at java.util.Spliterators$ArraySpliterator.tryAdvance(Spliterators.java:958)
	at java.util.stream.StreamSpliterators$WrappingSpliterator.lambda$initPartialTraversalState$0
```

#### Second parameter: a struct containing one or more [HiveDdbQueryFilter](src/main/java/com/klimber/hiveddbudtf/hive/HiveDdbQueryFilter.java)

This parameter is responsible for defining the query filters to use on the DynamoDB query.
It is designed as a wrapping `struct`, containing one or more `named_struct`, each representing one
[HiveDdbQueryFilter](src/main/java/com/klimber/hiveddbudtf/hive/HiveDdbQueryFilter.java).

```sql
struct(
    named_struct(
        'attribute', 'entityId',
        'attributeType', 'S',
        'operator', 'EQ',
        'value', local_data.entity_id
    ),
    named_struct(
        'attribute', 'version',
        'attributeType', 'N',
        'operator', 'GE',
        'value', local_data.version
    )
)
```

The wrapping `struct` is due to a limitation on `array` type, but you can consider it as being an array of filters.
Each filter will define the dynamoDB **attribute** it applies to, it's **[attributeType](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypeDescriptors)**,
which comparison **operator** to use, and which **value** should be used in the comparison. All filters are applied to the query using
`AND` logic, thus the example above query would be `entityId = local_data.entity_id AND version >= local_data.version`.

**Operators** currently supported are: `EQ` (equals), `GT` (greater than), `GE` (greater or equals),
`LT` (less than), `GE` (lesser or equals). More might be coming in the future.

The considerations below should be followed, which come from the [Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html)
DynamoDB API.
1. There **must** be exactly one filter for the partition key, and it's operator should be 'EQ'
2. Up to one filter can be included for the range key
3. Any quantity of filters can be defined for the remaining attributes.



## Limitations

### Nested `map<string,string>`

It's currently not possible to define nested maps of strings, for example: `array<map<string,string>>`.
This would cause the parser (borrowed from [emr-dynamodb-connector](https://github.com/awslabs/emr-dynamodb-connector/tree/master))
to try to parse it as a dynamoDB `ITEM`, resulting in the following exception:
```text
Caused by: org.apache.hadoop.hive.ql.metadata.HiveException: Hive Runtime Error while processing row {"entity_id":"0123456789TESTCODE"}
	at org.apache.hadoop.hive.ql.exec.MapOperator.process(MapOperator.java:570)
	at org.apache.hadoop.hive.ql.exec.tez.MapRecordSource.processRow(MapRecordSource.java:92)
	... 18 more
Caused by: java.lang.UnsupportedOperationException: DynamoDBItemType does not support this operation.
	at org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBItemType.getHiveData(HiveDynamoDBItemType.java:40)
	at org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser.getMapObject(DynamoDBDataParser.java:270)
	at org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBMapType.getHiveData(HiveDynamoDBMapType.java:79)
	at com.klimber.hiveddbudtf.HiveDdbQueryUdtf.toHiveData(HiveDdbQueryUdtf.java:131)
```

As a workaround, you could use structs and set the **hiveTypeMapping** as `arra<struct<my_field_1:string,my_field_2:string>>` instead.

## Motivation

Amazon already offers a tool to integrate DynamoDB and EMR, called
[emr-dynamodb-connector](https://github.com/awslabs/emr-dynamodb-connector/tree/master), which comes
pre-installed on Amazon EMR. It enables the [creation of an external table](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/EMRforDynamoDB.ExternalTableForDDB.html)
that connects Hive and EMR, allowing data to be transferred to/from DynamoDB. While it is an excellent
tool to write to DynamoDB and perform table dumps, querying using anything but a fixed value in the
partition key result in full table scans, resulting in slow and expensive operations. For example:

This SQL would perform a query operation correctly
```sql
SELECT * FROM ddb_external_table WHERE pkAttribute = 'pkValue'
```
Whereas this simple change would perform a full table scan instead:
```sql
SELECT * FROM ddb_external_table WHERE pkAttribute IN ('pkValue1', 'pkValue2')
```
This would also perform a full table scan:
```sql
SELECT * FROM ddb_external_table WHERE pkAttribute IN (SELECT pk_value FROM some_table)
```
This would also perform a full table scan:
```sql
SELECT * FROM some_table
LEFT JOIN ddb_external_table
    ON some_table.pkAttribute = ddb_external_table.pkAttribute
```

This lack of performance for query operations when the partition key is available, resulting in millions
of unnecessary records being fetched, is the main motivation behind HiveDdbQueryUDTF.

## FAQ

### How to control throughput?

While there is no builtin feature for this, the main variable affecting
thoughput for HiveDdbQueryUdtf is the number of mapper tasks. You can try to change settings like
`tez.grouping.split-count` to adjust the number for mapper tasks for your case.

### How to do cross-account queries?

Just like [emr-dynamodb-connector](https://github.com/awslabs/emr-dynamodb-connector/tree/master), you can set the `dynamodb.customAWSCredentialsProvider` to
use a custom AWS credentials provider. The custom credentials provider should implement `AWSCredentialsProvider`
and `Configurable`, check [CredentialsProviderTest](src/test/java/com/klimber/hiveddbudtf/client/ddb/CredentialsProviderTest.java)
for a dummy implementation.

**NOTE:** The custom credentials provider will receive the `Configuration` in the `setConf`
method. Take any information you need from the `Configuration` instance but do not save references 
to it in fields, or UDTF serialization will break.

**NOTE2:** Some other EMR features that accept custom credential providers, such as `s3a`, will pass
the `Configuration` instance via constructor. So it might be smart to consider having both an empty
constructor and a constructor that receives `Configuration` on your custom provider.

## Contributing

**Nothing here for now**, please reach out to me if you'd like to contribute.
