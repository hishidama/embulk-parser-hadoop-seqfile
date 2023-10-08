# Hadoop SequenceFile parser plugin for Embulk

Parses Hadoop SequenceFile read by other file input plugins.

## Overview

* **Plugin type**: parser
* **Guess supported**: no
* Embulk 0.10 or later
* jdk1.8 (jre1.8 is not supported) or Java9 later


## Example

### SequenceFile(key: Text, value: IntWritable)

```yaml
in:
  type: any file input plugin type
  parser:
    type: hadoop_seqfile
    key_class:   org.apache.hadoop.io.Text
    value_class: org.apache.hadoop.io.IntWritable
    columns:
    - {name: word, type: string, key: true, wtype: text}
    - {name: count, type: long, key: false, wtype: int}
```

### SequenceFile(key: NullWritable, value: Asakusa Framework DataModel)

```yaml
in:
  type: any file input plugin type
  parser:
    type: hadoop_seqfile
    value_class: com.example.asakusafw.dmdl.model.WordCount
    columns:
    - {name: word, type: string, wtype: stringOption}
    - {name: count, type: long, wtype: intOption}
```

See [asakusafw-helper.xlsx](asakusafw-helper.xlsx) as a tool to assist in generating columns from dmdl.


## Configuration

* **key_class**: key class name. (string, defualt: `org.apache.hadoop.io.NullWritable`)
* **value_class**: value class name. (string, defualt: `org.apache.hadoop.io.NullWritable`)
* **columns**: column definition. see below. (hash, required)
* **default_timezone**: default time zone. (string, default: `UTC`)
* **default_timestamp_format**: default timestemp format. (string, default: `%Y-%m-%d %H:%M:%S.%N %z`)
* **flush_count**: flush count. (int, default: `100`)

### columns

* **name**: Embulk column name. (string, required)
* **type**: Embulk column type. (string, required)
* **key**: key or value (`true` for key, `false` for value). (boolean, default: `false`)
* **wtype**: Writable type. (string, required)
* **timezone**: time zone. (string, default: **default_timezone**)
* **format**: timestemp format. (string, default: **default_timestamp_format**)

#### wtype (Writable type)

| wtype            | software          | Writable class                             |
|------------------|-------------------|--------------------------------------------|
| `null`           | Hadoop            | org.apache.hadoop.io.NullWritable          |
| `boolean`        | Hadoop            | org.apache.hadoop.io.BooleanWritable       |
| `byte`           | Hadoop            | org.apache.hadoop.io.ByteWritable          |
| `short`          | Hadoop            | org.apache.hadoop.io.ShortWritable         |
| `int`            | Hadoop            | org.apache.hadoop.io.IntWritable           |
| `long`           | Hadoop            | org.apache.hadoop.io.LongWritable          |
| `float`          | Hadoop            | org.apache.hadoop.io.FloatWritable         |
| `double`         | Hadoop            | org.apache.hadoop.io.DoubleWritable        |
| `vint`           | Hadoop            | org.apache.hadoop.io.VIntWritable          |
| `vlong`          | Hadoop            | org.apache.hadoop.io.VLongWritable         |
| `text`           | Hadoop            | org.apache.hadoop.io.Text                  |
| `booleanOption`  | Asakusa Framework | com.asakusafw.runtime.value.BooleanOption  |
| `byteOption`     | Asakusa Framework | com.asakusafw.runtime.value.ByteOption     |
| `shortOption`    | Asakusa Framework | com.asakusafw.runtime.value.ShortOption    |
| `intOption`      | Asakusa Framework | com.asakusafw.runtime.value.IntOption      |
| `longOption`     | Asakusa Framework | com.asakusafw.runtime.value.LongOption     |
| `floatOption`    | Asakusa Framework | com.asakusafw.runtime.value.FloatOption    |
| `doubleOption`   | Asakusa Framework | com.asakusafw.runtime.value.DoubleOption   |
| `decimalOption`  | Asakusa Framework | com.asakusafw.runtime.value.DecimalOption  |
| `stringOption`   | Asakusa Framework | com.asakusafw.runtime.value.StringOption   |
| `dateOption`     | Asakusa Framework | com.asakusafw.runtime.value.DateOption     |
| `datetimeOption` | Asakusa Framework | com.asakusafw.runtime.value.DateTimeOption |


## Install

1. install plugin
   ```
   $ mvn dependency:get -Dartifact=io.github.hishidama.embulk:embulk-parser-hadoop-seqfile:0.1.0
   ```

2. add setting to $HOME/.embulk/embulk.properties
   ```
   plugins.parser.hadoop_seqfile=maven:io.github.hishidama.embulk:hadoop-seqfile:0.1.0
   ```


## Build

```
$ ./gradlew test
```

### Build to local Maven repository

```
./gradlew generatePomFileForMavenJavaPublication
mvn install -f build/publications/mavenJava/pom-default.xml
./gradlew publishToMavenLocal
```

