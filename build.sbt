name := "metabolic-core"

version := "SNAPSHOT"

scalaVersion := "2.12.18"

/* Reusable versions */
val sparkVersion = "3.5.4"
val awsVersion = "1.12.682"
val icebergVersion = "1.7.1"
val testContainersVersion = "0.40.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "3.9.0",

  "io.delta" %% "delta-spark" % "3.3.0",
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion,
  "org.apache.iceberg" % "iceberg-aws-bundle" % icebergVersion,

  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "com.typesafe" % "config" % "1.4.0",
  "commons-lang" % "commons-lang" % "2.6",

  "net.liftweb" %% "lift-json" % "3.5.0",
  "io.lemonlabs" %% "scala-uri" % "1.4.10",
  "org.yaml" % "snakeyaml" % "2.3",

  "com.amazonaws" % "aws-java-sdk-s3" % awsVersion % Provided,
  "com.amazonaws" % "aws-java-sdk-secretsmanager" % awsVersion % Provided,
  "com.amazonaws" % "aws-java-sdk-glue" % awsVersion % Provided,
  "com.amazonaws" % "aws-java-sdk-kinesis" % awsVersion % Provided,
  "com.amazonaws" % "aws-java-sdk-athena" % awsVersion % Provided,
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "com.typesafe.play" %% "play-json" % "2.9.4",
  "io.starburst.openx.data" % "json-serde" % "1.3.9-e.10"
)

pomIncludeRepository := { x => false }

/* Uncomment for glue libs
resolvers += "aws-glue-etl-artifacts" at "https://aws-glue-etl-artifacts.s3.amazonaws.com/release/"
libraryDependencies += "com.amazonaws" % "AWSGlueETL" % "1.0.0" % Provided
*/

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5",
    "org.antlr" % "antlr4-runtime" % "4.8",
    "org.scala-lang.modules" %% "scala-xml" % "1.3.0"
  )
}

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % Test

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % s"3.5.3_2.0.1" % Test
dependencyOverrides += "org.xerial.snappy" % "snappy-java" % "1.1.10.7" % Test

libraryDependencies += "com.dimafeng" %% "testcontainers-scala-scalatest" % testContainersVersion % Test
libraryDependencies += "com.dimafeng" %% "testcontainers-scala-mysql" % testContainersVersion % Test
libraryDependencies += "com.dimafeng" %% "testcontainers-scala-kafka" % testContainersVersion % Test

libraryDependencies += "com.dimafeng" %% "testcontainers-scala-localstack" % testContainersVersion % Test
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4" % Test

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33" % "test"

libraryDependencies += "org.mockito" % "mockito-core" % "2.21.0" % "test"

coverageHighlighting := true

Test / fork := true
//Test / coverageEnabled  := true
//Test / parallelExecution := false
/* https://lists.apache.org/thread/814cpb1rpp73zkhtv9t4mkzzrznl82yn */
Test / javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "-XX:+UseG1GC",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

/* https://lists.apache.org/thread/814cpb1rpp73zkhtv9t4mkzzrznl82yn */
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)
scalacOptions += "-target:jvm-17"
scalacOptions ++= Seq("-deprecation", "-unchecked")