import Dependencies._

ThisBuild / scalaVersion     := "2.12.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "projectthree",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
    // https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
    libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12",
    // https://mvnrepository.com/artifact/commons-io/commons-io
    libraryDependencies += "commons-io" % "commons-io" % "2.8.0"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
