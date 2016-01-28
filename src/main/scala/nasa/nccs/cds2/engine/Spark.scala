package nasa.nccs.cds2.engine
import nccs.esgf.process.TaskRequest
// import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SparkEngine

object SparkEngine {
  val logger = LoggerFactory.getLogger( classOf[SparkEngine] )

//  lazy val conf = {
//    new SparkConf(false)
//      .setMaster("local[*]")
//      .setAppName("cdas")
//      .set("spark.logConf", "true")
//  }
//
//  lazy val sc = SparkContext.getOrCreate(conf)

  def execute( request: TaskRequest, run_args: Map[String,Any] ) = {
    logger.info("Execute { request: " + request.toString + ", runargs: " + run_args.toString + "}"  )
  }
}
