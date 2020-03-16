name := "ComputeApplicationEntrypoint"
version := "1.0"
scalaVersion := "2.11.12"
sbtVersion := "1.0"
val sparkVersion = "2.4.5"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)
mainClass := Some("com.huawei.compute.ComputeApplicationEntrypoint")
