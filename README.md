# cel2sql

cel2sql converts [CEL (Common Expression Language)](https://opensource.google/projects/cel) to SQL condition.
It is specifically targeting BigQuery standard SQL.

## Usage

```go
import (
    "context"
    "fmt"
    
    "cloud.google.com/go/bigquery"
    "github.com/cockscomb/cel2sql"
    "github.com/cockscomb/cel2sql/bq"
    "github.com/cockscomb/cel2sql/sqltypes"
    "github.com/google/cel-go/cel"
    "github.com/google/cel-go/checker/decls"
)

// BigQuery table metadata
var client *bigquery.Client = ...
tableMetadata, _ := client.Dataset("your_dataset").Table("employees").Metadata(context.TODO())

// Prepare CEL environment
env, _ := cel.NewEnv(
    cel.CustomTypeProvider(bq.NewTypeProvider(map[string]bigquery.Schema{
        "Employee": tableMetadata.Schema,
    })),
    sqltypes.SQLTypeDeclarations,
    cel.Declarations(
        decls.NewVar("employee", decls.NewObjectType("Employee")),
    ),
)

// Convert CEL to SQL
ast, _ := env.Compile(`employee.name == "John Doe" && employee.hired_at >= current_timestamp() - duration("24h")`)
sqlCondition, _ := cel2sql.Convert(ast)

fmt.Println(sqlCondition) // `employee`.`name` = "John Doe" AND `employee`.`hired_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
```

## Type Conversion

CEL Type    | BigQuery Standard SQL Data Type
----------- | ----------------------------------
`int`       | `INT64`
`uint`      | Unsupported but treated as `INT64`
`double`    | `FLOAT64`
`bool`      | `BOOL`
`string`    | `STRING`
`bytes`     | `BYTES`
`list`      | `ARRAY`
`map`       | `STRUCT`
`null_type` | `NULL`
`timestamp` | `TIMESTAMP`
`duration`  | `INTERVAL` 

## Supported CEL Operators/Functions

<table style="width: 100%; border: solid 1px;">
  <col style="width: 15%;">
  <col style="width: 40%;">
  <col style="width: 45%;">
  <tr>
    <th>Symbol</th>
    <th>Type</th>
    <th>SQL</th>
  </tr>
  <tr>
    <th rowspan="1">
      !_
    </th>
    <td>
      (bool) -> bool
    </td>
    <td>
      <code>NOT</code> bool
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      -_
    </th>
    <td>
      (int) -> int
    </td>
    <td>
      <code>-</code>int
    </td>
  </tr>
  <tr>
    <td>
      (double) -> double
    </td>
    <td>
      <code>-</code>double
    </td>
  </tr>
  <tr>
    <th rowspan="3">
      _!=_
    </th>
    <td>
      (A, A) -> bool
    </td>
    <td>
      A <code>!=</code> A
    </td>
  </tr>
  <tr>
    <td>
      (bool, bool) -> bool
    </td>
    <td>
      bool <code>IS NOT</code> bool
    </td>
  </tr>
  <tr>
    <td>
      (A, null) -> bool
    </td>
    <td>
      A <code>IS NOT NULL</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      _%_
    </th>
    <td>
      (int, int) -> int
    </td>
    <td>
      <code>MOD(</code>int<code>, </code>int<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      _&&_
    </th>
    <td>
      (bool, bool) -> bool
    </td>
    <td>
      bool <code>AND</code> bool
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      _*_
    </th>
    <td>
      (int, int) -> int
    </td>
    <td>
      int <code>*</code> int
    </td>
  </tr>
  <tr>
    <td>
      (double, double) -> double
    </td>
    <td>
      double <code>*</code> double
    </td>
  </tr>
  <tr>
    <th rowspan="7">
      _+_
    </th>
    <td>
      (int, int) -> int
    </td>
    <td>
      int <code>+</code> int 
    </td>
  </tr>
  <tr>
    <td>
      (double, double) -> double
    </td>
    <td>
      double <code>+</code> double 
    </td>
  </tr>
  <tr>
    <td>
      (string, string) -> string
    </td>
    <td>
      string <code>||</code> string 
    </td>
  </tr>
  <tr>
    <td>
      (bytes, bytes) -> bytes
    </td>
    <td>
      bytes <code>||</code> bytes 
    </td>
  </tr>
  <tr>
    <td>
      (list(A), list(A)) -> list(A)
    </td>
    <td>
      list(A) <code>||</code> list(A) 
    </td>
  </tr>
  <tr>
    <td>
      (google.protobuf.Timestamp, google.protobuf.Duration) -> google.protobuf.Timestamp
    </td>
    <td>
      <code>TIMESTAMP_ADD(</code>timestamp<code>, INTERVAL </code>duration<code> date_part)</code>
    </td>
  </tr>
  <tr>
    <td>
      (google.protobuf.Duration, google.protobuf.Timestamp) -> google.protobuf.Timestamp
    </td>
    <td>
      <code>TIMESTAMP_ADD(</code>timestamp<code>, INTERVAL </code>duration<code> date_part)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="3">
      _-_
    </th>
    <td>
      (int, int) -> int
    </td>
    <td>
      int <code>-</code> int
    </td>
  </tr>
  <tr>
    <td>
      (double, double) -> double
    </td>
    <td>
      double <code>-</code> double
    </td>
  </tr>
  <tr>
    <td>
      (google.protobuf.Timestamp, google.protobuf.Duration) -> google.protobuf.Timestamp
    </td>
    <td>
      <code>TIMESTAMP_SUB(</code>timestamp<code>, INTERVAL </code>duration<code> date_part)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      _/_
    </th>
    <td>
      (int, int) -> int
    </td>
    <td>
      int <code>/</code> int
    </td>
  </tr>
  <tr>
    <td>
      (double, double) -> double
    </td>
    <td>
      double <code>/</code> double
    </td>
  </tr>
  <tr>
    <th rowspan="6">
      _<=_
    </th>
    <td>
      (bool, bool) -> bool
    </td>
    <td>
      bool <code><=</code> bool
    </td>
  </tr>
  <tr>
    <td>
      (int, int) -> bool
    </td>
    <td>
      int <code><=</code> int
    </td>
  </tr>
  <tr>
    <td>
      (double, double) -> bool
    </td>
    <td>
      double <code><=</code> double
    </td>
  </tr>
  <tr>
    <td>
      (string, string) -> bool
    </td>
    <td>
      string <code><=</code> string
    </td>
  </tr>
  <tr>
    <td>
      (bytes, bytes) -> bool
    </td>
    <td>
      bytes <code><=</code> bytes
    </td>
  </tr>
  <tr>
    <td>
      (google.protobuf.Timestamp, google.protobuf.Timestamp) -> bool
    </td>
    <td>
      timestamp <code><=</code> timestamp
    </td>
  </tr>
  <tr>
    <th rowspan="6">
      _<_
    </th>
    <td>
      (bool, bool) -> bool
    </td>
    <td>
      bool <code><</code> bool
    </td>
  </tr>
  <tr>
    <td>
      (int, int) -> bool
    </td>
    <td>
      int <code><</code> int
    </td>
  </tr>
  <tr>
    <td>
      (double, double) -> bool
    </td>
    <td>
      double <code><</code> double
    </td>
  </tr>
  <tr>
    <td>
      (string, string) -> bool
    </td>
    <td>
      string <code><</code> string
    </td>
  </tr>
  <tr>
    <td>
      (bytes, bytes) -> bool
    </td>
    <td>
      bytes <code><</code> bytes
    </td>
  </tr>
  <tr>
    <td>
      (google.protobuf.Timestamp, google.protobuf.Timestamp) -> bool
    </td>
    <td>
      timestamp <code><</code> timestamp
    </td>
  </tr>
  <tr>
    <th rowspan="3">
      _==_
    </th>
    <td>
      (A, A) -> bool
    </td>
    <td>
      A <code>=</code> A
    </td>
  </tr>
  <tr>
    <td>
      (bool, bool) -> bool
    </td>
    <td>
      A <code>IS</code> A
    </td>
  </tr>
  <tr>
    <td>
      (A, null) -> bool
    </td>
    <td>
      A <code>IS NULL</code>
    </td>
  </tr>
  <tr>
    <th rowspan="6">
      _>=_
    </th>
    <td>
      (bool, bool) -> bool
    </td>
    <td>
      bool <code>>=</code> bool
    </td>
  </tr>
  <tr>
    <td>
      (int, int) -> bool
    </td>
    <td>
      int <code>>=</code> int
    </td>
  </tr>
  <tr>
    <td>
      (double, double) -> bool
    </td>
    <td>
      double <code>>=</code> double
    </td>
  </tr>
  <tr>
    <td>
      (string, string) -> bool
    </td>
    <td>
      string <code>>=</code> string
    </td>
  </tr>
  <tr>
    <td>
      (bytes, bytes) -> bool
    </td>
    <td>
      bytes <code>>=</code> bytes
    </td>
  </tr>
  <tr>
    <td>
      (google.protobuf.Timestamp, google.protobuf.Timestamp) -> bool
    </td>
    <td>
      timestamp <code>>=</code> timestamp
    </td>
  </tr>
  <tr>
    <th rowspan="6">
      _>_
    </th>
    <td>
      (bool, bool) -> bool
    </td>
    <td>
      bool <code>></code> bool
    </td>
  </tr>
  <tr>
    <td>
      (int, int) -> bool
    </td>
    <td>
      int <code>></code> int
    </td>
  </tr>
  <tr>
    <td>
      (double, double) -> bool
    </td>
    <td>
      double <code>></code> double
    </td>
  </tr>
  <tr>
    <td>
      (string, string) -> bool
    </td>
    <td>
      string <code>></code> string
    </td>
  </tr>
  <tr>
    <td>
      (bytes, bytes) -> bool
    </td>
    <td>
      bytes <code>></code> bytes
    </td>
  </tr>
  <tr>
    <td>
      (google.protobuf.Timestamp, google.protobuf.Timestamp) -> bool
    </td>
    <td>
      timestamp <code>></code> timestamp
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      _?_:_
    </th>
    <td>
      (bool, A, A) -> A
    </td>
    <td>
      <code>IF(</code>bool<code>, </code>A<code>, </code>A<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      _[_]
    </th>
    <td>
      (list(A), int) -> A
    </td>
    <td>
      list<code>[OFFSET(</code>int<code>)]</code>
    </td>
  </tr>
  <tr>
    <td>
      (map(A, B), A) -> B
    </td>
    <td>
      map<code>.`</code>A<code>`</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      in
    </th>
    <td>
      (A, list(A)) -> bool
    </td>
    <td>
      A <code>IN UNNEST(</code>list<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      _||_
    </th>
    <td>
      (bool, bool) -> bool
    </td>
    <td>
      bool <code>OR</code> bool
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      bool
    </th>
    <td>
      (int) -> bool
    </td>
    <td>
      <code>CAST(</code>int<code> AS BOOL)</code>
    </td>
  </tr>
  <tr>
    <td>
      (string) -> bool
    </td>
    <td>
      <code>CAST(</code>string<code> AS BOOL)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      bytes
    </th>
    <td>
      (string) -> bytes
    </td>
    <td>
      <code>CAST(</code>string<code>AS BYTES)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      contains
    </th>
    <td>
      string.(string) -> bool
    </td>
    <td>
      <code>INSTR(</code>string<code>, </code>string<code>) != 0</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      double
    </th>
    <td>
      (int) -> double
    </td>
    <td>
      <code>CAST(</code>int<code> AS FLOAT64)</code>
    </td>
  </tr>
  <tr>
    <td>
      (string) -> double
    </td>
    <td>
      <code>CAST(</code>string<code> AS FLOAT64)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      duration
    </th>
    <td>
      (string) -> google.protobuf.Duration
    </td>
    <td>
      <code>INTERVAL </code>duration<code> date_part</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      endsWith
    </th>
    <td>
      string.(string) -> bool
    </td>
    <td>
      <code>ENDS_WITH(</code>string<code>, </code>string<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      getDate
    </th>
    <td>
      google.protobuf.Timestamp.() -> int
    </td>
    <td>
      <code>EXTRACT(DAY FROM </code>timestamp<code>)</code>
    </td>
  </tr>
  <tr>
    <td>
      google.protobuf.Timestamp.(string) -> int
    </td>
    <td>
      <code>EXTRACT(DAY FROM </code>timestamp<code> AT </code>string<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      getDayOfMonth
    </th>
    <td>
      google.protobuf.Timestamp.() -> int
    </td>
    <td>
      <code>EXTRACT(DAY FROM </code>timestamp<code>) - 1</code>
    </td>
  </tr>
  <tr>
    <td>
      google.protobuf.Timestamp.(string) -> int
    </td>
    <td>
      <code>EXTRACT(DAY FROM </code>timestamp<code> AT </code>string<code>) - 1</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      getDayOfWeek
    </th>
    <td>
      google.protobuf.Timestamp.() -> int
    </td>
    <td>
      <code>EXTRACT(DAYOFWEEK FROM </code>timestamp<code>) - 1</code>
    </td>
  </tr>
  <tr>
    <td>
      google.protobuf.Timestamp.(string) -> int
    </td>
    <td>
      <code>EXTRACT(DAYOFWEEK FROM </code>timestamp<code> AT </code>string<code>) - 1</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      getDayOfYear
    </th>
    <td>
      google.protobuf.Timestamp.() -> int
    </td>
    <td>
      <code>EXTRACT(DAYOFYEAR FROM </code>timestamp<code>) - 1</code>
    </td>
  </tr>
  <tr>
    <td>
      google.protobuf.Timestamp.(string) -> int
    </td>
    <td>
      <code>EXTRACT(DAYOFYEAR FROM </code>timestamp<code> AT </code>string<code>) - 1</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      getFullYear
    </th>
    <td>
      google.protobuf.Timestamp.() -> int
    </td>
    <td>
      <code>EXTRACT(YEAR FROM </code>timestamp<code>)</code>
    </td>
  </tr>
  <tr>
    <td>
      google.protobuf.Timestamp.(string) -> int
    </td>
    <td>
      <code>EXTRACT(YEAR FROM </code>timestamp<code> AT </code>string<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      getHours
    </th>
    <td>
      google.protobuf.Timestamp.() -> int
    </td>
    <td>
      <code>EXTRACT(HOUR FROM </code>timestamp<code>)</code>
    </td>
  </tr>
  <tr>
    <td>
      google.protobuf.Timestamp.(string) -> int
    </td>
    <td>
      <code>EXTRACT(HOUR FROM </code>timestamp<code> AT </code>string<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      getMilliseconds
    </th>
    <td>
      google.protobuf.Timestamp.() -> int
    </td>
    <td>
      <code>EXTRACT(MILLISECOND FROM </code>timestamp<code>)</code>
    </td>
  </tr>
  <tr>
    <td>
      google.protobuf.Timestamp.(string) -> int
    </td>
    <td>
      <code>EXTRACT(MILLISECOND FROM </code>timestamp<code> AT </code>string<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      getMinutes
    </th>
    <td>
      google.protobuf.Timestamp.() -> int
    </td>
    <td>
      <code>EXTRACT(MINUTE FROM </code>timestamp<code>)</code>
    </td>
  </tr>
  <tr>
    <td>
      google.protobuf.Timestamp.(string) -> int
    </td>
    <td>
      <code>EXTRACT(MINUTE FROM </code>timestamp<code> AT </code>string<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      getMonth
    </th>
    <td>
      google.protobuf.Timestamp.() -> int
    </td>
    <td>
      <code>EXTRACT(MONTH FROM </code>timestamp<code>) - 1</code>
    </td>
  </tr>
  <tr>
    <td>
      google.protobuf.Timestamp.(string) -> int
    </td>
    <td>
      <code>EXTRACT(MONTH FROM </code>timestamp<code> AT </code>string<code>) - 1</code>
    </td>
  </tr>
  <tr>
    <th rowspan="2">
      getSeconds
    </th>
    <td>
      google.protobuf.Timestamp.() -> int
    </td>
    <td>
      <code>EXTRACT(SECOND FROM </code>timestamp<code>)</code>
    </td>
  </tr>
  <tr>
    <td>
      google.protobuf.Timestamp.(string) -> int
    </td>
    <td>
      <code>EXTRACT(SECOND FROM </code>timestamp<code> AT </code>string<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="4">
      int
    </th>
    <td>
      (bool) -> int
    </td>
    <td>
      <code>CAST(</code>bool<code> AS INT64)</code>
    </td>
  </tr>
  <tr>
    <td>
      (double) -> int
    </td>
    <td>
      <code>CAST(</code>double<code> AS INT64)</code>
    </td>
  </tr>
  <tr>
    <td>
      (string) -> int
    </td>
    <td>
      <code>CAST(</code>string<code> AS INT64)</code>
    </td>
  </tr>
  <tr>
    <td>
      (google.protobuf.Timestamp) -> int
    </td>
    <td>
      <code>UNIX_SECONDS(</code>timestamp<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      matches
    </th>
    <td>
      string.(string) -> bool
    </td>
    <td>
      <code>REGEXP_CONTAINS(</code>string<code>, </code>string<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="3">
      size
    </th>
    <td>
      (string) -> int
    </td>
    <td>
      <code>CHAR_LENGTH(</code>string<code>)</code>
    </td>
  </tr>
  <tr>
    <td>
      (bytes) -> int
    </td>
    <td>
      <code>BYTE_LENGTH(</code>bytes<code>)</code>
    </td>
  </tr>
  <tr>
    <td>
      (list(A)) -> int
    </td>
    <td>
      <code>ARRAY_LENGTH(</code>list<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      startsWith
    </th>
    <td>
      string.(string) -> bool
    </td>
    <td>
      <code>STARTS_WITH</code>string<code>, </code>string<code>)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="5">
      string
    </th>
    <td>
      (bool) -> string
    </td>
    <td>
      <code>CAST(</code>bool<code> AS STRING)</code>
    </td>
  </tr>
  <tr>
    <td>
      (int) -> string
    </td>
    <td>
      <code>CAST(</code>int<code> AS STRING)</code>
    </td>
  </tr>
  <tr>
    <td>
      (double) -> string
    </td>
    <td>
      <code>CAST(</code>double<code> AS STRING)</code>
    </td>
  </tr>
  <tr>
    <td>
      (bytes) -> string
    </td>
    <td>
      <code>CAST(</code>bytes<code> AS STRING)</code>
    </td>
  </tr>
  <tr>
    <td>
      (timestamp) -> string
    </td>
    <td>
      <code>CAST(</code>timestamp<code> AS STRING)</code>
    </td>
  </tr>
  <tr>
    <th rowspan="1">
      timestamp
    </th>
    <td>
      (string) -> google.protobuf.Timestamp
    </td>
    <td>
      <code>TIMESTAMP(</code>string<code>)</code>
    </td>
  </tr>
</table>

## Standard SQL Types/Functions

cel2sql supports time related types bellow.

- `DATE`
- `TIME`
- `DATETIME`

cel2sql contains time related functions bellow.

- `current_date()`
- `current_time()`
- `current_datetime()`
- `current_timestamp()`
- `interval(N, date_part)`
