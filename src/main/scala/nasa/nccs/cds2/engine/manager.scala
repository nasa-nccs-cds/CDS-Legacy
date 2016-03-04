package nasa.nccs.cds2.engine
import java.io.{PrintWriter, StringWriter}
import nasa.nccs.cdapi.cdm
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cds2.loaders.Collections
import nasa.nccs.esgf.process._
import org.nd4j.linalg.factory.Nd4j
import org.slf4j.LoggerFactory
import scala.collection.{concurrent, mutable}
import nasa.nccs.utilities.cdsutils
import nasa.nccs.cds2.kernels.kernelManager
import nasa.nccs.cdapi.kernels.{ Kernel, KernelModule, ExecutionResult, ExecutionContext, ExecutionResults }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Promise, Await, Future}
import scala.util.{ Try, Success, Failure }
import spray.caching._

class CollectionDataCacheMgr extends nasa.nccs.esgf.process.DataLoader {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val fragmentCache: Cache[PartitionedFragment] = LruCache()
  val datasetCache: Cache[CDSDataset] = LruCache()
  val variableCache: Cache[CDSVariable] = LruCache()
  var datasets = concurrent.TrieMap[String,CDSDataset]()

  def makeKey( collection: String, varName: String ) = collection + ":" + varName

  def extractFuture[T]( key: String, result: Option[Try[T]]): T = result match {
    case Some(tryVal) => tryVal match { case Success(x) => x; case Failure(t) => throw t }
    case None => throw new Exception(s"Error getting cache value $key")
  }

  def getDatasetFuture( collection: String, varName: String ): Future[CDSDataset] = {
    datasetCache( makeKey( collection, varName ) ) { produceDataset( collection, varName ) }
  }
  def getDataset( collection: String, varName: String ): CDSDataset = {
    val futureDataset: Future[CDSDataset] = getDatasetFuture( collection, varName )
    Await.result( futureDataset, Duration.Inf )
  }

  private def produceDataset( collection: String, varName: String )(p: Promise[CDSDataset]): Unit = {
    val datasetName = collection.toLowerCase
    Collections.CreateIP.get(datasetName) match {
      case Some(collection) =>
        val dataset_uid = collection.getUri(varName)
        datasets.get(dataset_uid) match {
          case Some(dataset) => dataset
          case None =>
            val dataset: CDSDataset = CDSDataset.load( datasetName, collection, varName )
            datasets += dataset_uid -> dataset
            p.success(dataset)
        }
      case None => p.failure( new Exception("Undefined collection for dataset " + varName + ", collection = " + collection) )
    }
  }

  private def promiseVariable(collection: String, varName: String)(p: Promise[CDSVariable]): Unit = {
    getDatasetFuture(collection, varName) onComplete {
      case Success(dataset) =>
        try {  p.success( dataset.loadVariable(varName) )  }
        catch { case e: Exception => p.failure(e) }
      case Failure(t) => p.failure(t)
    }
  }
  def getVariableFuture( collection: String, varName: String ): Future[CDSVariable] = {
    variableCache( makeKey( collection, varName ) ) { promiseVariable( collection, varName ) }
  }
  def getVariable( collection: String, varName: String ): CDSVariable = {
    val futureVariable: Future[CDSVariable] = getVariableFuture( collection, varName )
    Await.result( futureVariable, Duration.Inf )
  }

  private def promiseFragment( fragSpec: DataFragmentSpec )(p: Promise[PartitionedFragment]): Unit = {
    getVariableFuture( fragSpec.collection, fragSpec.varname )  onComplete {
      case Success(variable) =>
        try {  p.success( variable.loadRoi( fragSpec ) )  }
        catch { case e: Exception => p.failure(e) }
      case Failure(t) => p.failure(t)
    }
  }
  def getFragmentFuture( fragSpec: DataFragmentSpec  ): Future[PartitionedFragment] = {
    fragmentCache( fragSpec ) { promiseFragment( fragSpec ) }
  }
  def getFragment( fragSpec: DataFragmentSpec  ): PartitionedFragment = {
    val futureFragment: Future[PartitionedFragment] = getFragmentFuture( fragSpec )
    val fragment = Await.result( futureFragment, Duration.Inf )
    logger.info("Loaded variable (%s:%s) subset data, shape = %s ".format(fragSpec.collection, fragSpec.varname, fragment.shape.toString))
    fragment
  }
}

object collectionDataCache extends CollectionDataCacheMgr()

class CDS2ExecutionManager {
  val collectionDataManager = new DataManager( collectionDataCache )
  val logger = LoggerFactory.getLogger(this.getClass)
  private var futureResult: Option[Future[xml.Elem]] = None
  val processAsyncResult: PartialFunction[Try[xml.Elem],Unit] = {
    case n @ Success(_) => println( "Process Completed: " + n.toString )
    case e @ Failure(_) => println( "Process Error: " + e.toString )
  }

  def getKernelModule( moduleName: String  ): KernelModule = {
    kernelManager.getModule( moduleName  ) match {
      case Some(kmod) => kmod
      case None => throw new Exception("Unrecognized Kernel Module %s, modules = %s ".format( moduleName, kernelManager.getModuleNames.mkString("[ ",", "," ]") ) )
    }
  }
  def getKernel( moduleName: String, operation: String  ): Kernel = {
    val kmod = getKernelModule( moduleName )
    kmod.getKernel( operation  ) match {
      case Some(kernel) => kernel
      case None => throw new Exception( s"Unrecognized Kernel %s in Module %s, kernels = %s ".format( operation, moduleName, kmod.getKernelNames.mkString("[ ",", "," ]")) )
    }
  }
  def getKernel( kernelName: String  ): Kernel = {
    val toks = kernelName.split('.')
    getKernel( toks.dropRight(1).mkString("."), toks.last )
  }

  def fatal(err: Exception): xml.Elem = {
    logger.error( "\nError Executing Kernel: %s\n".format(err.getMessage) )
    val sw = new StringWriter
    err.printStackTrace(new PrintWriter(sw))
    logger.error( sw.toString )
    <Error> { err.getMessage } </Error>
  }

  def futureExecute( request: TaskRequest, run_args: Map[String,String] ): Future[xml.Elem] = Future {
    try {
      for (data_container <- request.variableMap.values; if data_container.isSource) {
        collectionDataManager.loadVariableData(data_container, request.getDomain(data_container.getSource))
      }
      executeWorkflows(request, run_args).toXml
    } catch {
      case err: Exception => fatal(err)
    }
  }

  def execute( request: TaskRequest, run_args: Map[String,String] ): xml.Elem = {
    logger.info("Execute { runargs: " + run_args.toString + ",  request: " + request.toString + " }")
    val async = run_args.getOrElse("async", "false").toBoolean
    futureResult = Option( this.futureExecute( request, run_args ) )
    if(async) { asyncResult;  <result> </result> }
    else awaitResult
  }

  def asyncResult: Unit = {
    futureResult match {
      case None => Unit
      case Some( result ) => result onComplete processAsyncResult
    }
  }

  def awaitResult: xml.Elem = {
    futureResult match {
      case None => <Error> { "No result" } </Error>
      case Some( result ) => Await.result( result, Duration.Inf )
    }
  }

  def describeProcess( kernelName: String ): xml.Elem = getKernel( kernelName ).toXml

  def listProcesses(): xml.Elem = kernelManager.toXml

  def executeWorkflows( request: TaskRequest, run_args: Map[String,String] ): ExecutionResults = {
    new ExecutionResults( request.workflows.map( workflow => workflow.operations.map( operation => operationExecution( operation, request.domainMap,  run_args ) ) ).flatten )
  }

  def operationExecution(operation: OperationContainer, domainMap: Map[String,DomainContainer], run_args: Map[String, String]): ExecutionResult = {
    getKernel( operation.name.toLowerCase ).execute( getExecutionContext(operation, domainMap, run_args) )
  }
  def getExecutionContext( operation: OperationContainer, domainMap: Map[String,DomainContainer], run_args: Map[String, String] ): ExecutionContext = {
    val fragments: List[PartitionedFragment] = operation.inputs.map(collectionDataManager.getVariableData(_))
    val binArrayOpt = collectionDataManager.getBinnedArrayFactory( operation )
    val args = operation.optargs ++ run_args
    new ExecutionContext( fragments, binArrayOpt, domainMap, collectionDataManager, args )
  }
}

object SampleTaskRequests {

  def getAveTimeseries: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val workflows = List[WorkflowContainer]( new WorkflowContainer( operations = List( new OperationContainer( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), optargs = Map("axis" -> "t") )  ) ) )
    val variableMap = Map[String,DataContainer]( "v0" -> new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) ) )
    val domainMap = Map[String,DomainContainer]( "d0" -> new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Lev,1,1), DomainAxis(Lat,100,100), DomainAxis(Lon,100,100) ) ) )
    new TaskRequest( "CDS.average", variableMap, domainMap, workflows )
  }

  def getTimeAveSlice: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lon" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lev" -> Map("start" -> 8, "end" -> 8, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "hur:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "( v0, axes: t )")))
      TaskRequest( "CDS.average", dataInputs )
  }

  def getYearlyCycleSlice: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "( v0, axes: t, bins: t|month|ave|year )")))
    TaskRequest( "CDS.bin", dataInputs )
  }
  def getCreateVRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")),
                        Map("name" -> "d1", "time" -> Map("start" -> 3, "end" -> 3, "system" -> "indices") ) ),
      "variable" -> List( Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0") ),
      "operation" -> List(Map("unparsed" -> "CDS.anomaly( v0, axes: t ),CDS.bin( v0, axes: t, bins: t|month|ave|year ),CDS.subset( v0, domain:d1 )" )) )
    TaskRequest( "CDS.workflow", dataInputs )
  }

  def getSubsetRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")),
        Map("name" -> "d1", "time" -> Map("start" -> 3, "end" -> 3, "system" -> "indices") ) ),
      "variable" -> List( Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0") ),
      "operation" -> List(Map("unparsed" -> "( v0, domain:d1 )" )) )
    TaskRequest( "CDS.subset", dataInputs )
  }

  def getTimeSliceAnomaly: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lat" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lon" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lev" -> Map("start" -> 8, "end" -> 8, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "hur:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "( v0, axes: t )")))
    TaskRequest( "CDS.anomaly", dataInputs )
  }

  def getCacheRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0",  "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")) )
    TaskRequest( "util.cache", dataInputs )
  }

  def getSpatialAve: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lev" -> Map("start" -> 8, "end" -> 8, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "( v0, axes: xy, y.weights: inverse_cosine )")))
    TaskRequest( "CDS.average", dataInputs )
  }

  def getAnomalyTest: TaskRequest = {
    val dataInputs = Map(
      "domain" ->  List(Map("name" -> "d0", "lat" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lon" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lev" -> Map("start" -> 8, "end" -> 8, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "hur:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "(v0,axes:t)")))
    TaskRequest( "CDS.anomaly", dataInputs )
  }

  def getAveArray: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val workflows = List[WorkflowContainer]( new WorkflowContainer( operations = List( new OperationContainer( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), optargs = Map("axis" -> "xy") )  ) ) )
    val variableMap = Map[String,DataContainer]( "v0" -> new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) ) )
    val domainMap = Map[String,DomainContainer]( "d0" -> new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Lev,1,1), DomainAxis(Lat,100,100) ) ) )
    new TaskRequest( "CDS.average", variableMap, domainMap, workflows )
  }

}

object executionTest extends App {
  val request = SampleTaskRequests.getCreateVRequest
  val async = false
  val run_args = Map( "async" -> async.toString )
  val cds2ExecutionManager = new CDS2ExecutionManager()
  val result = cds2ExecutionManager.execute(request, run_args)
  println(result.toString)
  cds2ExecutionManager.awaitResult
}

object parseTest extends App {
  val axes = "c,,,"
  val r = axes.split(",").map(_.head).toList
  println( r )
}

object arrayTest extends App {
  val array = Nd4j.create( Array.fill[Float](100)(1f), Array(50,1,2))
  val s0 = array.slice(0, 0 ); println( "shape = %s".format( s0.shape.mkString(",")) )
}



//  TaskRequest: name= CWT.average, variableMap= Map(v0 -> DataContainer { id = hur:v0, dset = merra/mon/atmos, domain = d0 }, ivar#1 -> OperationContainer { id = ~ivar#1,  name = , result = ivar#1, inputs = List(v0), optargs = Map(axis -> xy) }), domainMap= Map(d0 -> DomainContainer { id = d0, axes = List(DomainAxis { id = lev, start = 0, end = 1, system = "indices", bounds =  }) })

