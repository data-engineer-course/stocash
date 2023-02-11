ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.3.2",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.17.7",
  "org.apache.hadoop" % "hadoop-aws" % "3.2.2",
  "org.springframework.vault" % "spring-vault-core" % "2.3.1",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "3.3.1_1.4.0" % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "Task2Spark"
  )
