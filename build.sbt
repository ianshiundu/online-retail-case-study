ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val sparkVersion = "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "online-retail-job",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.typesafe" % "config" % "1.4.1",
      "com.crealytics" %% "spark-excel" % "3.3.1_0.18.5",
      "org.scalatest" %% "scalatest" % "3.1.1" % Test,
    ),
  )
