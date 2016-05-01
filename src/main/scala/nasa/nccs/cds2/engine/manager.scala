package nasa.nccs.cds2.engine
import java.io.{PrintWriter, StringWriter}
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cds2.loaders.Collections
import nasa.nccs.esgf.process._
import org.slf4j.{ LoggerFactory, Logger }
import scala.concurrent.ExecutionContext.Implicits.global
import nasa.nccs.utilities.cdsutils
import nasa.nccs.cds2.kernels.KernelMgr
import nasa.nccs.cdapi.kernels._
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise, Await}
import scala.util.{ Try, Success, Failure }
import java.util.concurrent.atomic.AtomicReference
import spray.caching._


class Counter(start: Int = 0) {
  private val index = new AtomicReference(start)
  def get: Int = {
    val i0 = index.get
    if(index.compareAndSet( i0, i0 + 1 )) i0 else get
  }
}

class CollectionDataCacheMgr extends nasa.nccs.esgf.process.DataLoader {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  private val fragmentCache: Cache[PartitionedFragment] = LruCache()
  private val datasetCache: Cache[CDSDataset] = LruCache()
  private val variableCache: Cache[CDSVariable] = LruCache()

  def makeKey(collection: String, varName: String) = collection + ":" + varName

  def extractFuture[T](key: String, result: Option[Try[T]]): T = result match {
    case Some(tryVal) => tryVal match {
      case Success(x) => x;
      case Failure(t) => throw t
    }
    case None => throw new Exception(s"Error getting cache value $key")
  }

  def getDatasetFuture(collection: String, varName: String): Future[CDSDataset] = {
    datasetCache(makeKey(collection, varName)) {
      produceDataset(collection, varName) _
    }
  }

  def getDataset(collection: String, varName: String): CDSDataset = {
    val futureDataset: Future[CDSDataset] = getDatasetFuture(collection, varName)
    Await.result(futureDataset, Duration.Inf)
  }

  private def produceDataset(collection: String, varName: String)(p: Promise[CDSDataset]): Unit = {
    val datasetName = collection.toLowerCase
    Collections.datasets.get(datasetName) match {
      case Some(collection) =>
        val dataset = CDSDataset.load(datasetName, collection, varName)
        logger.info("Completed reading dataset (%s:%s) ".format( collection, varName ))
        p.success(dataset)
      case None => p.failure(new Exception("Undefined collection for dataset " + varName + ", collection = " + collection))
    }
  }

  private def promiseVariable(collection: String, varName: String)(p: Promise[CDSVariable]): Unit = {
    getDatasetFuture(collection, varName) onComplete {
      case Success(dataset) =>
        try {
          val variable = dataset.loadVariable(varName)
          logger.info("Completed reading variable %s ".format( varName ))
          p.success(variable)
        }
        catch {
          case e: Exception => p.failure(e)
        }
      case Failure(t) => p.failure(t)
    }
  }

  def getVariableFuture(collection: String, varName: String): Future[CDSVariable] = variableCache(makeKey(collection, varName)) {
    promiseVariable(collection, varName) _
  }

  def getVariable(collection: String, varName: String): CDSVariable = {
    val futureVariable: Future[CDSVariable] = getVariableFuture(collection, varName)
    Await.result(futureVariable, Duration.Inf)
  }

  def getVariable(fragSpec: DataFragmentSpec): CDSVariable = getVariable(fragSpec.collection, fragSpec.varname)

  private def cutExistingFragment( fragSpec: DataFragmentSpec, abortSizeFraction: Float=0f ): Option[PartitionedFragment] = findEnclosingFragSpec(fragSpec.getKey, FragmentSelectionCriteria.Smallest ) match {
    case Some( fkey: DataFragmentKey) => getExistingFragment(fkey) match {
      case Some(fragmentFuture) =>
        if (!fragmentFuture.isCompleted && (fkey.getSize * abortSizeFraction > fragSpec.getSize)) {
          logger.info( "Cache Chunk[%s] found but not yet ready, abandoning cache access attempt".format( fkey.shape.mkString(",")) )
          None
        } else {
          val fragment = Await.result(fragmentFuture, Duration.Inf)
          Some(fragment.cutNewSubset(fragSpec.roi))
        }
      case None => cutExistingFragment( fragSpec, abortSizeFraction )
    }
    case None => None
  }

  private def promiseFragment( fragSpec: DataFragmentSpec )(p: Promise[PartitionedFragment]): Unit = {
    getVariableFuture( fragSpec.collection, fragSpec.varname )  onComplete {
      case Success(variable) =>
        try {
          val t0 = System.nanoTime()
          val result: PartitionedFragment = variable.loadRoi( fragSpec )
          logger.info("Completed variable (%s:%s) subset data input in time %.4f sec, section = %s ".format(fragSpec.collection, fragSpec.varname, (System.nanoTime()-t0)/1.0E9, fragSpec.roi ))
//          logger.info("Data column = [ %s ]".format( ( 0 until result.shape(0) ).map( index => result.getValue( Array(index,0,100,100) ) ).mkString(", ") ) )
          p.success( result )
        } catch { case e: Exception => p.failure(e) }
      case Failure(t) => p.failure(t)
    }
  }

  private def clearRedundantFragments( fragSpec: DataFragmentSpec ) = findEnclosedFragSpecs(fragSpec.getKey).map(_.toString).foreach( fragmentCache.remove( _ ) )

  private def getFragmentFuture( fragSpec: DataFragmentSpec  ): Future[PartitionedFragment] = {
    val fragFuture = fragmentCache( fragSpec.getKey ) { promiseFragment( fragSpec ) _ }
    fragFuture onComplete { case Success(fragment) => clearRedundantFragments( fragSpec ); case Failure(t) => Unit }
    logger.info( ">>>>>>>>>>>>>>>> Put frag in cache: " + fragSpec.toString + ", keys = " + fragmentCache.keys.mkString("[",",","]") )
    fragFuture
  }

  def getFragment( fragSpec: DataFragmentSpec, abortSizeFraction: Float=0f  ): PartitionedFragment = cutExistingFragment( fragSpec, abortSizeFraction ) getOrElse {
    val fragmentFuture = getFragmentFuture( fragSpec )
    val result = Await.result( fragmentFuture, Duration.Inf )
    logger.info("Loaded variable (%s:%s) subset data, section = %s ".format(fragSpec.collection, fragSpec.varname, fragSpec.roi ))
    result
  }

  def getFragmentAsync( fragSpec: DataFragmentSpec  ): Future[PartitionedFragment] = cutExistingFragment(fragSpec) match {
    case Some( fragment ) => Future { fragment }
    case None => getFragmentFuture( fragSpec )
  }

  def loadOperationInputFuture( dataContainer: DataContainer, domain_container: DomainContainer ): Future[OperationInputSpec] = {
    val variableFuture = getVariableFuture(dataContainer.getSource.collection, dataContainer.getSource.name)
    variableFuture.flatMap( variable => {
      val fragSpec = variable.createFragmentSpec(domain_container.axes)
      val axisSpecs: AxisIndices = variable.getAxisIndices(dataContainer.getOpSpecs)
      for (frag <- getFragmentFuture(fragSpec)) yield new OperationInputSpec( fragSpec, axisSpecs)
    })
  }

  def loadDataFragmentFuture( dataContainer: DataContainer, domain_container: DomainContainer ): Future[PartitionedFragment] = {
    val variableFuture = getVariableFuture(dataContainer.getSource.collection, dataContainer.getSource.name)
    variableFuture.flatMap( variable => {
      val fragSpec = variable.createFragmentSpec(domain_container.axes)
      for (frag <- getFragmentFuture(fragSpec)) yield frag
    })
  }

  def getExistingFragment( fkey: DataFragmentKey  ): Option[Future[PartitionedFragment]] = {
    val rv: Option[Future[PartitionedFragment]] = fragmentCache.get( fkey )
    logger.info( ">>>>>>>>>>>>>>>> Get frag from cache: search key = " + fkey.toString + ", existing keys = " + fragmentCache.keys.mkString("[",",","]") + ", Success = " + rv.isDefined.toString )
    rv
  }

  def getFragSpecsForVariable( collection: String, varName: String ): Set[DataFragmentKey] = fragmentCache.keys.filter(
    _ match {
      case fkey: DataFragmentKey => fkey.sameVariable(collection,varName)
      case x => logger.warn("Unexpected fragment key type: " + x.getClass.getName); false
    }).map( _ match { case fkey: DataFragmentKey => fkey } )

  def findEnclosingFragSpecs( fkey: DataFragmentKey, admitEquality: Boolean = true ): Set[DataFragmentKey] = {
    val variableFrags = getFragSpecsForVariable( fkey.collection, fkey.varname )
    variableFrags.filter( fkeyParent => fkeyParent.contains( fkey, admitEquality ) )
  }
  def findEnclosedFragSpecs( fkeyParent: DataFragmentKey, admitEquality: Boolean = false ): Set[DataFragmentKey] = {
    val variableFrags = getFragSpecsForVariable( fkeyParent.collection, fkeyParent.varname )
    variableFrags.filter( fkey => fkeyParent.contains( fkey, admitEquality ) )
  }

  def findEnclosingFragSpec( fkeyChild: DataFragmentKey, selectionCriteria: FragmentSelectionCriteria.Value, admitEquality: Boolean = true ): Option[DataFragmentKey] = {
    val enclosingFragments = findEnclosingFragSpecs( fkeyChild, admitEquality )
    if ( enclosingFragments.isEmpty ) None else Some( selectionCriteria match {
      case FragmentSelectionCriteria.Smallest => enclosingFragments.minBy(_.getRoi.computeSize())
      case FragmentSelectionCriteria.Largest  => enclosingFragments.maxBy(_.getRoi.computeSize())
    } )
  }
}

object collectionDataCache extends CollectionDataCacheMgr()

class CDS2ExecutionManager( val serverConfiguration: Map[String,String] ) {
  val serverContext = new ServerContext( collectionDataCache, serverConfiguration )
  val logger = LoggerFactory.getLogger(this.getClass)
  val kernelManager = new KernelMgr()
  private val counter = new Counter

  def getKernelModule( moduleName: String  ): KernelModule = {
    kernelManager.getModule( moduleName ) match {
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

  def fatal(err: Throwable): String = {
    logger.error( "\nError Executing Kernel: %s\n".format(err.getMessage) )
    val sw = new StringWriter
    err.printStackTrace(new PrintWriter(sw))
    logger.error( sw.toString )
    err.getMessage
  }

  def loadInputData( request: TaskRequest, run_args: Map[String,String] ): RequestContext = {
    val sourceContainers = request.variableMap.values.filter(_.isSource)
    val sources = for (data_container: DataContainer <- request.variableMap.values; if data_container.isSource)
      yield serverContext.loadVariableData(data_container, request.getDomain(data_container.getSource) )
    new RequestContext( request.domainMap, Map(sources.toSeq:_*), run_args )
  }

  def futureExecute( request: TaskRequest, run_args: Map[String,String] ): Future[ExecutionResults] = Future {
    val requestContext = loadInputData( request, run_args )
    executeWorkflows( request, requestContext )
  }

  def blockingExecute( request: TaskRequest, run_args: Map[String,String] ): xml.Elem =  {
    logger.info("Blocking Execute { runargs: " + run_args.toString + ",  request: " + request.toString + " }")
    val t0 = System.nanoTime
    try {
      val requestContext = loadInputData( request, run_args )
      val t1 = System.nanoTime
      val rv = executeWorkflows( request, requestContext ).toXml
      val t2 = System.nanoTime
      logger.info( "Execute Completed: LoadVariablesT> %.4f, ExecuteWorkflowT> %.4f, totalT> %.4f ".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9, (t2-t0)/1.0E9 ) )
      rv
    } catch {
      case err: Exception => <error> {fatal(err)} </error>
    }
  }

//  def futureExecute( request: TaskRequest, run_args: Map[String,String] ): Future[xml.Elem] = Future {
//    try {
//      val sourceContainers = request.variableMap.values.filter(_.isSource)
//      val inputFutures: Iterable[Future[OperationInputSpec]] = for (data_container: DataContainer <- request.variableMap.values; if data_container.isSource) yield {
//        serverContext.dataLoader.loadVariableDataFuture(data_container, request.getDomain(data_container.getSource))
//      }
//      inputFutures.flatMap( inputFuture => for( input <- inputFuture ) yield executeWorkflows(request, run_args).toXml )
//    } catch {
//      case err: Exception => fatal(err)
//    }
//  }

  def getResultFilePath( resultId: String ): Option[String] = {
    import java.io.File
    val resultFile = Kernel.getResultFile( serverConfiguration, resultId )
    if(resultFile.exists) Some(resultFile.getAbsolutePath) else None
  }

  def executeAsync( request: TaskRequest, run_args: Map[String,String] ): ( String, Future[ExecutionResults] ) = {
    logger.info("Execute { runargs: " + run_args.toString + ",  request: " + request.toString + " }")
    val async = run_args.getOrElse("async", "false").toBoolean
    val resultId = "r" + counter.get.toString
    val futureResult = this.futureExecute( request, Map( "resultId" -> resultId ) ++ run_args )
    futureResult onSuccess { case results: ExecutionResults =>
      println("Process Completed: " + results.toString )
      processAsyncResult( resultId, results )
    }
    futureResult onFailure { case e: Throwable => fatal( e ); throw e }
    (resultId, futureResult)
  }

  def processAsyncResult( resultId: String, results: ExecutionResults ) = {

  }

//  def execute( request: TaskRequest, runargs: Map[String,String] ): xml.Elem = {
//    val async = runargs.getOrElse("async","false").toBoolean
//    if(async) executeAsync( request, runargs ) else  blockingExecute( request, runargs )
//  }

  def describeProcess( kernelName: String ): xml.Elem = getKernel( kernelName ).toXml

  def listProcesses(): xml.Elem = kernelManager.toXml

  def executeWorkflows( request: TaskRequest, requestCx: RequestContext ): ExecutionResults = {
    new ExecutionResults( request.workflows.flatMap(workflow => workflow.operations.map( operationExecution( _, requestCx ))) )
  }

  def executeUtility( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
    val result: xml.Node =  <result> {"Completed executing utility " + operationCx.name.toLowerCase } </result>
    new XmlExecutionResult( operationCx.name.toLowerCase + "~u0", result )
  }

  def operationExecution( operationCx: OperationContext, requestCx: RequestContext ): ExecutionResult = {
    val opName = operationCx.name.toLowerCase
    val module_name = opName.split('.')(0)
    module_name match {
      case "util" => executeUtility( operationCx, requestCx, serverContext )
      case x => getKernel( opName ).execute( operationCx, requestCx, serverContext )
    }
  }
}

object SampleTaskRequests {

  def getAveTimeseries: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val workflows = List[WorkflowContainer]( new WorkflowContainer( operations = List( new OperationContext( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), Map("axis" -> "t") ) ) ) )
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
                        Map("name" -> "d1", "time" -> Map("start" -> "2010-01-16T12:00:00", "end" -> "2010-01-16T12:00:00", "system" -> "values") ) ),
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
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "( v0, axes: t )")))
    TaskRequest( "CDS.anomaly", dataInputs )
  }

  def getMetadataRequest( level: Int ): TaskRequest = {
    val dataInputs: Map[String, Seq[Map[String, Any]]] = level match {
      case 0 => Map()
      case 1 => Map( "variable" -> List ( Map( "uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0" ) ) )
    }
    TaskRequest( "CDS.metadata", dataInputs )
  }

  def getCacheRequest: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0",  "lev" -> Map("start" -> 100000, "end" -> 100000, "system" -> "values"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")) )
    TaskRequest( "util.cache", dataInputs )
  }

  def getSpatialAve: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lev" -> Map("start" -> 20, "end" -> 20, "system" -> "indices"), "time" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "( v0, axes: xy )")))
    TaskRequest( "CDS.average", dataInputs )
  }

  def getMax: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lev" -> Map("start" -> 20, "end" -> 20, "system" -> "indices"), "time" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "( v0, axes: xy )")))
    TaskRequest( "CDS.max", dataInputs )
  }

  def getMin: TaskRequest = {
    val dataInputs = Map(
      "domain" -> List( Map("name" -> "d0", "lev" -> Map("start" -> 20, "end" -> 20, "system" -> "indices"), "time" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://merra/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "( v0, axes: xy )")))
    TaskRequest( "CDS.min", dataInputs )
  }

  def getAnomalyTest: TaskRequest = {
    val dataInputs = Map(
      "domain" ->  List(Map("name" -> "d0", "lat" -> Map("start" -> -7.0854263, "end" -> -7.0854263, "system" -> "values"), "lon" -> Map("start" -> -122.075, "end" -> -122.075, "system" -> "values"), "lev" -> Map("start" -> 100000, "end" -> 100000, "system" -> "values"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "(v0,axes:t)")))
    TaskRequest( "CDS.anomaly", dataInputs )
  }

  def getAnomalyArrayTest: TaskRequest = {
    val dataInputs = Map(
      "domain" ->  List( Map("name" -> "d1", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")), Map("name" -> "d0", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lon" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lev" -> Map("start" -> 30, "end" -> 30, "system" -> "indices"))),
      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
      "operation" -> List(Map("unparsed" -> "CDS.anomaly(v0,axes:t),CDS.subset(v0,domain:d1)")))
    TaskRequest( "CDS.workflow", dataInputs )
  }

  def getAveArray: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val workflows = List[WorkflowContainer]( new WorkflowContainer( operations = List( new OperationContext( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), Map("axis" -> "xy")  ) ) ) )
    val variableMap = Map[String,DataContainer]( "v0" -> new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) ) )
    val domainMap = Map[String,DomainContainer]( "d0" -> new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Lev,4,4), DomainAxis(Lat,100,100) ) ) )
    new TaskRequest( "CDS.average", variableMap, domainMap, workflows )
  }

  def getFragmentSyncFuture( dataContainer: DataContainer, domain_container: DomainContainer): Future[PartitionedFragment] = Future {
    getFragmentSync( dataContainer, domain_container ) match {
      case Some(pf) => pf
      case None => new PartitionedFragment()
    }
  }

  def getFragmentSync( dataContainer: DataContainer, domainContainer: DomainContainer): Option[PartitionedFragment] = {
    val datasetName = dataContainer.getSource.collection.toLowerCase
    var varName = dataContainer.getSource.name
    Collections.datasets.get(datasetName) match {
      case Some(collection) =>
        val dataset = CDSDataset.load(datasetName, collection, varName)
        val variable = dataset.loadVariable(varName)
        val t0 = System.nanoTime
        val fragSpec = variable.createFragmentSpec(domainContainer.axes)
        val axisIndices: AxisIndices = variable.getAxisIndices(dataContainer.getOpSpecs)
        val result = variable.loadRoi( fragSpec )
        val t1 = System.nanoTime
        println( " ** Frag gen time %.4f".format( (t1-t0)/1.0E9 ) )
        Some( result )
      case None => None
    }
  }
}

object exeSyncTest extends App {
  import nasa.nccs.esgf.process.DomainAxis.Type._
  val operationContainer =  new OperationContext( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), Map("axis" -> "xy") )
  val dataContainer = new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) )
  val domainContainer = new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Lev,6,6) ) )
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val t0 = System.nanoTime
  val partitionedFragmentOpt = SampleTaskRequests.getFragmentSync( dataContainer, domainContainer )
  val t1 = System.nanoTime
  partitionedFragmentOpt match {
    case Some( partitionedFragment ) => println( "Got Value, time = %.4f: %s: ".format( (t1-t0)/1.0E9, partitionedFragment.toString ) )
    case None => println( "Error" )
  }
}

object exeConcurrencyTest extends App {
  import nasa.nccs.esgf.process.DomainAxis.Type._
  val operationContainer =  new OperationContext( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), Map("axis" -> "xy") )
  val dataContainer = new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) )
  val domainContainer = new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Lev,10,10) ) )
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  cds2ExecutionManager.serverContext.dataLoader.getVariable( dataContainer.getSource.collection, dataContainer.getSource.name )
  val t0 = System.nanoTime
//  val futurePartitionedFragment: Future[PartitionedFragment] = cds2ExecutionManager.serverContext.dataLoader.loadDataFragmentFuture( dataContainer, domainContainer )
  val futurePartitionedFragment: Future[PartitionedFragment]  = SampleTaskRequests.getFragmentSyncFuture( dataContainer, domainContainer )
  val t1 = System.nanoTime
  println("Got Future, time = %.4f".format((t1-t0)/1.0E9))
  val partitionedFragment: PartitionedFragment = Await.result( futurePartitionedFragment, Duration.Inf )
  val t2 = System.nanoTime
  println( "Got Value, time = %.4f (%.4f): %s: ".format( (t2-t1)/1.0E9, (t2-t0)/1.0E9,partitionedFragment.toString ) )
}

object executionTest extends App {
  val request = SampleTaskRequests.getAnomalyTest
  val async = false
  val run_args = Map( "async" -> async.toString )
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val t0 = System.nanoTime
  if(async) {
    cds2ExecutionManager.executeAsync(request, run_args) match {
      case ( resultId: String, futureResult: Future[ExecutionResults] ) =>
        val t1 = System.nanoTime
        println ("Initial Result, time = %.4f ".format ((t1 - t0) / 1.0E9) )
        val result = Await.result (futureResult, Duration.Inf)
        val t2 = System.nanoTime
        println ("Final Result, time = %.4f, result = %s ".format ((t2 - t1) / 1.0E9, result.toString) )
      case x => println( "Unrecognized result from executeAsync: " + x.toString )
    }
   }
  else {
    val t1 = System.nanoTime
    val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
    val t2 = System.nanoTime
    println("Final Result, time = %.4f (%.4f): %s ".format( (t2-t1)/1.0E9, (t2-t0)/1.0E9, final_result.toString) )
  }
}

object execCacheTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getCacheRequest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result) )
}

object execMetadataTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getMetadataRequest(0)
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result) )
}

object execAnomalyTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getAnomalyArrayTest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result) )
}

object execSpatialAveTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getSpatialAve
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result) )
}

object execMaxTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getMax
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result) )
}

object execMinTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )
  val request = SampleTaskRequests.getMin
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result) )
}

object execAnomalyWithCacheTest extends App {
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  val run_args = Map( "async" -> "false" )

  println( ">>>>>>>>>>>>>>>>>>>>>>>>>> Start CACHE REQUEST "  )
  val t1 = System.nanoTime
  val cache_request = SampleTaskRequests.getCacheRequest
  val cache_result = cds2ExecutionManager.blockingExecute(cache_request, run_args)
  val t2 = System.nanoTime
  println( ">>>>>>>>>>>>>>>>>>>>>>>>>> Cache1: %.4f".format((t2-t1)/1.0E9) )

  val cache_request1 = SampleTaskRequests.getCacheRequest
  val cache_result1 = cds2ExecutionManager.blockingExecute(cache_request, run_args)
  val t3 = System.nanoTime
  println( ">>>>>>>>>>>>>>>>>>>>>>>>>> Cache2: %.4f".format((t3-t2)/1.0E9) )


  val request = SampleTaskRequests.getAnomalyTest
  val final_result = cds2ExecutionManager.blockingExecute(request, run_args)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  println( ">>>> Final Result: " + printer.format(final_result) )
}

object parseTest extends App {
  val axes = "c,,,"
  val r = axes.split(",").map(_.head).toList
  println( r )
}





//  TaskRequest: name= CWT.average, variableMap= Map(v0 -> DataContainer { id = hur:v0, dset = merra/mon/atmos, domain = d0 }, ivar#1 -> OperationContext { id = ~ivar#1,  name = , result = ivar#1, inputs = List(v0), optargs = Map(axis -> xy) }), domainMap= Map(d0 -> DomainContainer { id = d0, axes = List(DomainAxis { id = lev, start = 0, end = 1, system = "indices", bounds =  }) })

