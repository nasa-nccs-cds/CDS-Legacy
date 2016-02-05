val kernelPackages = settingKey[ Seq[String] ]("A list of user-defined Kernel packages")

name := "CDS2"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"

organization := "nasa.nccs"

kernelPackages := Seq[String]( "nasa.nccs.cds2.modules.CDS" )

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, kernelPackages),
    buildInfoPackage := "cdsbt"
  )

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// resolvers += "Unidata maven repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"

// resolvers ++= Seq( "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases" )

libraryDependencies ++= Dependencies.scala

libraryDependencies ++= Dependencies.ndarray

// libraryDependencies ++= Dependencies.spark



    