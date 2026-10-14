ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day22-batch-mini-project",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.9",
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
