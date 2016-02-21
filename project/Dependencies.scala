import sbt._

object Version {
  val hadoop    = "2.6.0"
  val logback   = "1.1.3"
  val mockito   = "1.10.19"
  val scala     = "2.11.7"
  val scalaTest = "2.2.4"
  val slf4j     = "1.7.6"
  val spark     = "1.4.1"
}

object Library {
  val hadoopClient   = "org.apache.hadoop" %  "hadoop-client"   % Version.hadoop
  val logbackClassic = "ch.qos.logback"    %  "logback-classic" % Version.logback
  val mockitoAll     = "org.mockito"       %  "mockito-all"     % Version.mockito
  val scalaTest      = "org.scalatest"     %% "scalatest"       % Version.scalaTest
  val slf4jApi       = "org.slf4j"         %  "slf4j-api"       % Version.slf4j
  val sparkSQL       = "org.apache.spark"  %% "spark-sql"       % Version.spark
  val sparkCore      = "org.apache.spark"  %% "spark-core"      % "1.6.0"
  val scalaxml       = "org.scala-lang.modules" %% "scala-xml"  % "1.0.3"
  val scalaparser    = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3"

  val cdm            = "edu.ucar"           % "cdm"             % "4.4.0"
  val clcommon       = "edu.ucar"           % "clcommon"        % "4.4.0"
  val netcdf4        = "edu.ucar"           % "netcdf4"         % "4.4.0"
  val netcdfAll      = "edu.ucar"           % "netcdfAll"       % "4.6.4"
  val nd4s           = "org.nd4j"           % "nd4s_2.11"       % "0.4-rc3.8"
  val nd4j           =  "org.nd4j"          % "nd4j-x86"        % "0.4-rc3.8"
  val opendap        = "edu.ucar"           % "opendap"         % "2.2.2"
  val httpservices   = "edu.ucar"           %  "httpservices"   % "4.6.0"
  val udunits        = "edu.ucar"           %  "udunits"        % "4.6.0"
  val joda           = "joda-time"          % "joda-time"       % "2.8.1"
  val natty          = "com.joestelmach"    % "natty"           % "0.11"
  val guava          = "com.google.guava"   % "guava"           % "18.0"
  val breeze         = "org.scalanlp"      %% "breeze"          % "0.12"
  val kernelmod      = "nasa.nccs"         %% "kermodbase"      % "1.0-SNAPSHOT"
  val cdapi          = "nasa.nccs"         %% "CDAPI"           % "1.0-SNAPSHOT"
}

object Dependencies {
  import Library._

  val sparkAkkaHadoop = Seq(
    sparkSQL,
    hadoopClient,
    logbackClassic % "test",
    scalaTest      % "test",
    mockitoAll     % "test"
  )
  val scala = Seq( logbackClassic, joda, natty )

  val spark = Seq( sparkCore )

  val ndarray = Seq( nd4s, nd4j, breeze )

  val kernels = Seq( kernelmod, cdapi )
}










