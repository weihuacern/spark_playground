# Compute Engine in Scala

## Dependancies

- Apache Spark: [Apache Spark 2.4.5](https://apache.claz.org/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz).
- PostgreSQL JDBC Driver: [postgresql-42.2.5.jar](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.5/) in $SPARK_HOME/jars/ (/usr/local/spark/jars/).

## Commands

```bash
sbt compile
sbt run
sbt "run badAppName"
sbt "run StaticDataProcessor"
sbt "run DynamicDataProcessor"
sbt "run CoreDataGenerator"
sbt clean compile package
```

```bash
spark-submit --class com.huawei.compute.ComputeApplicationEntrypoint --master local[4] target/scala-2.11/computeapplicationentrypoint_2.11-1.0.jar StaticDataProcessor
spark-submit --class com.huawei.compute.ComputeApplicationEntrypoint --master local[4] target/scala-2.11/computeapplicationentrypoint_2.11-1.0.jar DynamicDataProcessor
spark-submit --class com.huawei.compute.ComputeApplicationEntrypoint --master local[4] target/scala-2.11/computeapplicationentrypoint_2.11-1.0.jar CoreDataGenerator
```
