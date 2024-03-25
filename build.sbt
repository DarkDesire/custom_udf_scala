// Databricks Runtime 14.3 LTS
// System environment
// Operating System: Ubuntu 22.04.3 LTS
// Java: Zulu 8.74.0.17-CA-linux64
// Scala: 2.12.15
// Python: 3.10.12
// R: 4.3.1
// Delta Lake: 3.1.0
// Spark 3.5.0
// Link: https://docs.databricks.com/en/release-notes/runtime/14.3lts.html

// SBT: 1.9.8
// JDK: (Azul Systems, Inc. Java 1.8.0_402)

val buildVersion = "0.1.0"
val buildName = "custom_udf_scala"
val buildOrganization = "com.example"
val buildOrganizationName = "example"
val buildScalaVersion = "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := buildName,
    version := buildVersion,
    organization := buildOrganization,
    organizationName := buildOrganizationName,
    scalaVersion := buildScalaVersion,
    compileOrder := CompileOrder.JavaThenScala,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    scalacOptions ++= Seq("-target:jvm-1.8"),
    resolvers += "Maven" at "https://repo1.maven.org/maven2",
  )