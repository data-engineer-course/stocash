ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
//  "org.postgresql" % "postgresql" % "42.5.0",
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.3.2"
)

lazy val root = (project in file("."))
  .settings(
    name := "Task2Spark"
  )
