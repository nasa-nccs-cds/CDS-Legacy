
name := "CDS2"

version := "1.0"

scalaVersion := "2.11.7"

organization := "nccs"

lazy val root = project in file(".")

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"

// resolvers ++= Seq( "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases" )

libraryDependencies ++= Dependencies.scala

libraryDependencies ++= Dependencies.ndarray

libraryDependencies ++= Dependencies.spark



    