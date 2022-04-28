ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "spark-testing"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-core" % "3.2.1" % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % "3.2.1",
  "org.apache.spark" %% "spark-sql" % "3.2.1" % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % "3.2.1",
  "org.apache.spark" %% "spark-catalyst" % "3.2.1" % Test classifier "tests",
  "org.scalatest" %% "scalatest" % "3.3.0-SNAP3" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.40.2" % "test",
  "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.40.2" % "test",
  "org.postgresql" % "postgresql" % "42.3.4"
)