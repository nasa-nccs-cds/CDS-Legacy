package nasa.nccs.cds2.engine
import java.io.{PrintWriter, StringWriter}
import nasa.nccs.cdapi.cdm
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.tensors.Nd4jMaskedTensor
import nasa.nccs.cds2.loaders.Collections
import nasa.nccs.esgf.process._
import org.nd4j.linalg.factory.Nd4j
import org.slf4j.LoggerFactory
import scala.collection.{concurrent, mutable}
import nasa.nccs.utilities.cdsutils
import nasa.nccs.cds2.kernels.KernelMgr
import nasa.nccs.cdapi.kernels._
import scala.concurrent.ExecutionContext.Implicits.global
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
    Collections.CreateIP.get(datasetName) match {
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

  def getVariableFuture(collection: String, varName: String): Future[CDSVariable] = {
    variableCache(makeKey(collection, varName)) {
      promiseVariable(collection, varName) _
    }
  }

  def getVariable(collection: String, varName: String): CDSVariable = {
    val futureVariable: Future[CDSVariable] = getVariableFuture(collection, varName)
    Await.result(futureVariable, Duration.Inf)
  }

  def getVariable(fragSpec: DataFragmentSpec): CDSVariable = getVariable(fragSpec.collection, fragSpec.varname)

  private def cutExistingFragment(fragSpec: DataFragmentSpec): Option[PartitionedFragment] = findEnclosingFragSpec(fragSpec, FragmentSelectionCriteria.Smallest ) match {
    case Some(enclosingFragSpec: DataFragmentSpec) => getExistingFragment(enclosingFragSpec) match {
      case Some(fragmentFuture) =>
        val fragment = Await.result(fragmentFuture, Duration.Inf)
        Some( fragment.cutNewSubset(fragSpec.roi) )
      case None => cutExistingFragment(fragSpec)
    }
    case None => None
  }


  private def promiseFragment( fragSpec: DataFragmentSpec )(p: Promise[PartitionedFragment]): Unit = {
    getVariableFuture( fragSpec.collection, fragSpec.varname )  onComplete {
      case Success(variable) =>
        try {
          val t0 = System.nanoTime()
          val result = variable.loadRoi( fragSpec )
          logger.info("Completed variable (%s:%s) subset data input in time %.4f sec, section = %s ".format(fragSpec.collection, fragSpec.varname, (System.nanoTime()-t0)/1.0E9, fragSpec.roi ))
          p.success( result )
        } catch { case e: Exception => p.failure(e) }
      case Failure(t) => p.failure(t)
    }
  }

  private def clearRedundantFragments( fragSpec: DataFragmentSpec ) = findEnclosedFragSpecs(fragSpec).foreach( fragmentCache.remove(_) )

  private def getFragmentFuture( fragSpec: DataFragmentSpec  ): Future[PartitionedFragment] = {
    val fragFuture = fragmentCache( fragSpec ) { promiseFragment( fragSpec ) _ }
    fragFuture onComplete { case Success(fragment) => clearRedundantFragments( fragSpec ); case Failure(t) => Unit }
    fragFuture
  }

  def getFragment( fragSpec: DataFragmentSpec  ): PartitionedFragment = {
    cutExistingFragment(fragSpec) getOrElse {
        val fragmentFuture = getFragmentFuture( fragSpec )
        val result = Await.result( fragmentFuture, Duration.Inf )
        logger.info("Loaded variable (%s:%s) existing subset data, section = %s ".format(fragSpec.collection, fragSpec.varname, fragSpec.roi ))
        result
    }
  }

  def getFragmentAsync( fragSpec: DataFragmentSpec  ): Future[PartitionedFragment] = {
    cutExistingFragment(fragSpec) match {
      case Some( fragment ) => Future { fragment }
      case None => getFragmentFuture( fragSpec )
    }
  }

  def loadOperationInputFuture( dataContainer: DataContainer, domain_container: DomainContainer ): Future[OperationInputSpec] = {
    val variableFuture = getVariableFuture(dataContainer.getSource.collection, dataContainer.getSource.name)
    variableFuture.flatMap( variable => {
      val fragSpec = variable.createFragmentSpec(domain_container.axes)
      val axisSpecs: AxisSpecs = variable.getAxisSpecs(dataContainer.getOpSpecs)
      for (frag <- getFragmentFuture(fragSpec)) yield new OperationInputSpec(fragSpec, axisSpecs)
    })
  }

  def loadDataFragmentFuture( dataContainer: DataContainer, domain_container: DomainContainer ): Future[PartitionedFragment] = {
    val variableFuture = getVariableFuture(dataContainer.getSource.collection, dataContainer.getSource.name)
    variableFuture.flatMap( variable => {
      val fragSpec = variable.createFragmentSpec(domain_container.axes)
      for (frag <- getFragmentFuture(fragSpec)) yield frag
    })
  }

  def getExistingFragment( fragSpec: DataFragmentSpec  ): Option[Future[PartitionedFragment]] = fragmentCache.get( fragSpec )

  def getFragSpecsForVariable( collection: String, varName: String ): Set[DataFragmentSpec] = fragmentCache.keys.filter(
    _ match {
      case frag: DataFragmentSpec => frag.sameVariable(collection,varName)
      case x => logger.warn("Unexpected fragment key type: " + x.getClass.getName); false
    }).asInstanceOf[Set[DataFragmentSpec]]

  def findEnclosingFragSpecs(targetFragSpec: DataFragmentSpec): Set[DataFragmentSpec] = {
    val variableFrags = getFragSpecsForVariable( targetFragSpec.collection, targetFragSpec.varname )
    variableFrags.filter( _.roi.contains(targetFragSpec.roi) )
  }
  def findEnclosedFragSpecs(targetFragSpec: DataFragmentSpec): Set[DataFragmentSpec] = {
    val variableFrags = getFragSpecsForVariable( targetFragSpec.collection, targetFragSpec.varname )
    variableFrags.filter( fragSpec => targetFragSpec.roi.contains( fragSpec.roi ) )
  }

  def findEnclosingFragSpec(targetFragSpec: DataFragmentSpec, selectionCriteria: FragmentSelectionCriteria.Value ): Option[DataFragmentSpec] = {
    val enclosingFragments = findEnclosingFragSpecs(targetFragSpec)
    if ( enclosingFragments.isEmpty ) None else Some( selectionCriteria match {
      case FragmentSelectionCriteria.Smallest => enclosingFragments.minBy(_.roi.computeSize())
      case FragmentSelectionCriteria.Largest  => enclosingFragments.maxBy(_.roi.computeSize())
    } )
  }
}

object collectionDataCache extends CollectionDataCacheMgr()

class CDS2ExecutionManager( val serverConfiguration: Map[String,String] ) {
  val collectionDataManager = new DataManager( collectionDataCache )
  val logger = LoggerFactory.getLogger(this.getClass)
  val kernelManager = new KernelMgr()
  private val counter = new Counter

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

  def fatal(err: Throwable): String = {
    logger.error( "\nError Executing Kernel: %s\n".format(err.getMessage) )
    val sw = new StringWriter
    err.printStackTrace(new PrintWriter(sw))
    logger.error( sw.toString )
    err.getMessage
  }

  def futureExecute( request: TaskRequest, run_args: Map[String,String] ): Future[ExecutionResults] = Future {
    val sourceContainers = request.variableMap.values.filter(_.isSource)
    for (data_container: DataContainer <- request.variableMap.values; if data_container.isSource) {
      collectionDataManager.loadVariableData(data_container, request.getDomain(data_container.getSource))
    }
    executeWorkflows(request, run_args)
  }

  def blockingExecute( request: TaskRequest, run_args: Map[String,String] ): xml.Elem =  {
    logger.info("Blocking Execute { runargs: " + run_args.toString + ",  request: " + request.toString + " }")
    try {
      val sourceContainers = request.variableMap.values.filter(_.isSource)
      for (data_container: DataContainer <- request.variableMap.values; if data_container.isSource) {
        collectionDataManager.loadVariableData(data_container, request.getDomain(data_container.getSource))
      }
      executeWorkflows(request, run_args).toXml
    } catch {
      case err: Exception => <error> {fatal(err)} </error>
    }
  }

//  def futureExecute( request: TaskRequest, run_args: Map[String,String] ): Future[xml.Elem] = Future {
//    try {
//      val sourceContainers = request.variableMap.values.filter(_.isSource)
//      val inputFutures: Iterable[Future[OperationInputSpec]] = for (data_container: DataContainer <- request.variableMap.values; if data_container.isSource) yield {
//        collectionDataManager.dataLoader.loadVariableDataFuture(data_container, request.getDomain(data_container.getSource))
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

  def executeWorkflows( request: TaskRequest, run_args: Map[String,String] ): ExecutionResults = {
    new ExecutionResults( request.workflows.flatMap(workflow => workflow.operations.map(operation => operationExecution(operation, request.domainMap, run_args))) )
  }

  def operationExecution(operation: OperationContainer, domainMap: Map[String,DomainContainer], run_args: Map[String, String]): ExecutionResult = {
    getKernel( operation.name.toLowerCase ).execute( getExecutionContext(operation, domainMap, run_args) )
  }
  def getExecutionContext( operation: OperationContainer, domainMap: Map[String,DomainContainer], run_args: Map[String, String] ): ExecutionContext = {
    val fragments: List[KernelDataInput] = for( uid <- operation.inputs ) yield new KernelDataInput( collectionDataManager.getVariableData(uid), collectionDataManager.getAxisSpecs(uid) )
    val binArrayOpt = collectionDataManager.getBinnedArrayFactory( operation )
    val args = operation.optargs ++ run_args
    new ExecutionContext( fragments, binArrayOpt, domainMap, collectionDataManager, serverConfiguration, args )
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
    Collections.CreateIP.get(datasetName) match {
      case Some(collection) =>
        val dataset = CDSDataset.load(datasetName, collection, varName)
        val variable = dataset.loadVariable(varName)
        val t0 = System.nanoTime
        val fragSpec = variable.createFragmentSpec(domainContainer.axes)
        val axisSpecs: AxisSpecs = variable.getAxisSpecs(dataContainer.getOpSpecs)
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
  val operationContainer =  new OperationContainer( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), optargs = Map("axis" -> "xy") )
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
  val operationContainer =  new OperationContainer( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), optargs = Map("axis" -> "xy") )
  val dataContainer = new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) )
  val domainContainer = new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Lev,10,10) ) )
  val cds2ExecutionManager = new CDS2ExecutionManager(Map.empty)
  cds2ExecutionManager.collectionDataManager.dataLoader.getVariable( dataContainer.getSource.collection, dataContainer.getSource.name )
  val t0 = System.nanoTime
//  val futurePartitionedFragment: Future[PartitionedFragment] = cds2ExecutionManager.collectionDataManager.dataLoader.loadDataFragmentFuture( dataContainer, domainContainer )
  val futurePartitionedFragment: Future[PartitionedFragment]  = SampleTaskRequests.getFragmentSyncFuture( dataContainer, domainContainer )
  val t1 = System.nanoTime
  println("Got Future, time = %.4f".format((t1-t0)/1.0E9))
  val partitionedFragment: PartitionedFragment = Await.result( futurePartitionedFragment, Duration.Inf )
  val t2 = System.nanoTime
  println( "Got Value, time = %.4f (%.4f): %s: ".format( (t2-t1)/1.0E9, (t2-t0)/1.0E9,partitionedFragment.toString ) )
}

object executionTest extends App {
  val request = SampleTaskRequests.getCreateVRequest
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

object parseTest extends App {
  val axes = "c,,,"
  val r = axes.split(",").map(_.head).toList
  println( r )
}

object arrayTest extends App {
  val array = Nd4j.create( Array.fill[Float](40)(1f), Array(10,2,2))
  val tensor = new Nd4jMaskedTensor(array)
  array.putScalar( Array(1,1,0), 3.0 )
  array.putScalar( Array(1,1,1), 3.0 )
  println( "init data = [ %s ]".format( tensor.data.mkString(",")) )
  val s0 = tensor.slice( 1, 0 )
  println( "shape = %s, offset = %d".format( s0.shape.mkString(","), s0.tensor.offset ) )
  s0.tensor.putScalar( Array(0,1,0), 2.0 )
  println( "data = [ %s ]".format( tensor.data.mkString(",")) )
  println( "data0 = %s".format( s0.tensor.getFloat( Array(0,0,0))) )
  println( "data1 = %s".format( s0.tensor.getFloat( Array(0,0,1))) )
}




//  TaskRequest: name= CWT.average, variableMap= Map(v0 -> DataContainer { id = hur:v0, dset = merra/mon/atmos, domain = d0 }, ivar#1 -> OperationContainer { id = ~ivar#1,  name = , result = ivar#1, inputs = List(v0), optargs = Map(axis -> xy) }), domainMap= Map(d0 -> DomainContainer { id = d0, axes = List(DomainAxis { id = lev, start = 0, end = 1, system = "indices", bounds =  }) })

