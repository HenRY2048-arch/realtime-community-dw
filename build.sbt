ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "Community_Real_Time",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.0.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0",
      "com.google.guava" % "guava" % "14.0.1",
      "com.redislabs" % "spark-redis_2.12" % "3.1.0",
      "io.delta" %% "delta-core" % "0.7.0",
      "redis.clients" % "jedis" % "3.3.0",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value
    ),
    run / fork := true,
    run / javaOptions ++= Seq(
      "-Dfile.encoding=UTF-8"
    )
  )
