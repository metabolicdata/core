name := "metabolic-core"

version := "SNAPSHOT"

scalaVersion := "2.12.17"

/* Reusable versions */
val sparkVersion = "3.3.0"
val awsVersion = "1.12.401"
val testContainersVersion = "0.40.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "3.3.2",

  "io.delta" %% "delta-core" % "2.1.0",

  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "com.typesafe" % "config" % "1.4.0",

  "net.liftweb" %% "lift-json" % "3.5.0",
  "io.lemonlabs" %% "scala-uri" % "1.4.10",
  "org.yaml" % "snakeyaml" % "1.29",

  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion % Provided,
  "com.amazonaws" % "aws-java-sdk-secretsmanager" % awsVersion % Provided,
  "com.amazonaws" % "aws-java-sdk-glue" % awsVersion % Provided,
  "com.amazonaws" % "aws-java-sdk-kinesis" % awsVersion % Provided,
  "com.amazonaws" % "aws-java-sdk-athena" % awsVersion % Provided,
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "io.starburst.openx.data" % "json-serde" % "1.3.9-e.10"
)

pomIncludeRepository := { x => false }

/* Uncomment for glue libs
resolvers += "aws-glue-etl-artifacts" at "https://aws-glue-etl-artifacts.s3.amazonaws.com/release/"
libraryDependencies += "com.amazonaws" % "AWSGlueETL" % "1.0.0" % Provided
*/

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0",
    "org.antlr" % "antlr4-runtime" % "4.8",
    "org.scala-lang.modules" %% "scala-xml" % "1.3.0"
  )
}

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % Test


libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.4.0" % Test
dependencyOverrides += "org.xerial.snappy" % "snappy-java" % "1.1.8.2" % Test

libraryDependencies += "com.dimafeng" %% "testcontainers-scala-scalatest" % testContainersVersion % Test
libraryDependencies += "com.dimafeng" %% "testcontainers-scala-mysql" % testContainersVersion % Test
libraryDependencies += "com.dimafeng" %% "testcontainers-scala-kafka" % testContainersVersion % Test

libraryDependencies += "com.dimafeng" %% "testcontainers-scala-localstack" % testContainersVersion % Test
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4" % Test

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.49" % "test"

libraryDependencies += "org.mockito" % "mockito-core" % "2.21.0" % "test"

Test / fork := true
Test / coverageEnabled  := true
Test / parallelExecution := false

coverageHighlighting := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M","-XX:+CMSClassUnloadingEnabled")
scalacOptions += "-target:jvm-1.8"
scalacOptions ++= Seq("-deprecation", "-unchecked")

