package nasa.nccs.cds2.engine

import nasa.nccs.cds2.cdm
import nasa.nccs.cds2.loaders.Collections
import nasa.nccs.esgf.process._
import nasa.nccs.esgf.engine.PluginExecutionManager
import org.nd4j.linalg.factory.Nd4j
import org.slf4j.LoggerFactory
import scala.collection.mutable
import nasa.nccs.cds2.utilities.cdsutils
import nasa.nccs.cds2.kernels.kernelManager
import nasa.nccs.cdapi.kernels.{ Kernel, KernelModule, ExecutionResult, ExecutionResults, DataFragment }


//object cds2PluginExecutionManager extends PluginExecutionManager {
//  val cds2ExecutionManager = new CDS2ExecutionManager()
//
//  override def execute( process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], run_args: Map[String,Any] ): xml.Elem = {
//    val request = TaskRequest( process_name, datainputs )
//    cds2ExecutionManager.execute( request, run_args )
//  }
//}


class CDS2ExecutionManager {
  val logger = LoggerFactory.getLogger(classOf[CDS2ExecutionManager])

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
      case None => throw new Exception( s"Unrecognized Kernel $operation in Module $moduleName ")
    }
  }
  def getKernel( kernelName: String  ): Kernel = {
    val toks = kernelName.split('.')
    getKernel( toks.dropRight(1).mkString("."), toks.last )
  }

  def execute( request: TaskRequest, run_args: Map[String,Any] ): xml.Elem = {
    logger.info("Execute { request: " + request.toString + ", runargs: " + run_args.toString + "}"  )
    val data_manager = new DataManager( request.domainMap )
    for( data_container <- request.variableMap.values; if data_container.isSource )  data_manager.loadVariableData( data_container.uid, data_container.getSource )
    executeWorkflows( request.workflows, data_manager, run_args ).toXml
  }

  def describeProcess( kernelName: String ): xml.Elem = getKernel( kernelName ).toXml

  def listProcesses(): xml.Elem = kernelManager.toXml

  def executeWorkflows( workflows: List[WorkflowContainer], data_manager: DataManager, run_args: Map[String,Any] ): ExecutionResults = {
    new ExecutionResults( workflows.map( workflow => workflow.operations.map( operation => operationExecution( operation, data_manager,  run_args ) ).flatten ).flatten )
  }

  def operationExecution(operation: OperationContainer, data_manager: DataManager, run_args: Map[String, Any]): List[ExecutionResult] = {
    val inputSubsets: List[DataFragment] = operation.inputs.map(data_manager.getVariableData(_))
    inputSubsets.map(inputSubset => { getKernel( operation.name.toLowerCase ).execute( inputSubsets, run_args) } )
  }
}

class DataManager( val domainMap: Map[String,DomainContainer] ) {
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.engine.DataManager")
  var datasets = mutable.Map[String,cdm.CDSDataset]()
  var subsets = mutable.Map[String,DataFragment]()

  def getDataset( data_source: DataSource ): cdm.CDSDataset = {
    val datasetName = data_source.collection.toLowerCase
    Collections.CreateIP.get(datasetName) match {
      case Some(collection) =>
        val dataset_uid = collection.getUri(data_source.name)
        datasets.get(dataset_uid) match {
          case Some(dataset) => dataset
          case None =>
            val dataset: cdm.CDSDataset = cdm.CDSDataset.load(datasetName, collection, data_source.name)
            datasets += dataset_uid -> dataset
            dataset
        }
      case None =>
        throw new Exception("Undefined collection for dataset " + data_source.name + ", collection = " + data_source.collection)
    }
  }

  def getVariableData(uid: String): DataFragment = {
    subsets.get(uid) match {
      case Some(subset) => subset
      case None => throw new Exception("Can't find subset Data for Variable $uid")
    }
  }

  def loadVariableData(uid: String, data_source: DataSource): DataFragment = {
    subsets.get(uid) match {
      case Some(subset) => subset
      case None =>
        val dataset: cdm.CDSDataset = getDataset(data_source)
        domainMap.get(data_source.domain) match {
          case Some(domain_container) =>
            val variable = dataset.loadVariable(data_source.name)
            val fragment = variable.loadRoi(domain_container.axes)
            subsets += uid -> fragment
            logger.info("Loaded variable %s (%s:%s) subset data, shape = %s ".format(uid, data_source.collection, data_source.name, fragment.shape.toString) )
            fragment
          case None =>
            throw new Exception("Undefined domain for dataset " + data_source.name + ", domain = " + data_source.domain)
        }
    }
  }
}

object SampleTaskRequests {

  def getAnomalyTimeseries: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val workflows = List[WorkflowContainer]( new WorkflowContainer( operations = List( new OperationContainer( identifier = "CDS.average~ivar#1",  name ="CDS.average", result = "ivar#1", inputs = List("v0"), optargs = Map("axis" -> "xy") )  ) ) )
    val variableMap = Map[String,DataContainer]( "v0" -> new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) ) )
    val domainMap = Map[String,DomainContainer]( "d0" -> new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Lev,1,1), DomainAxis(Lat,100,100), DomainAxis(Lon,100,100) ) ) )
    new TaskRequest( "CDS.anomaly", variableMap, domainMap, workflows )
  }

  def getCacheChunk: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val variableMap = Map[String,DataContainer]( "v0" -> new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) ) )
    val domainMap = Map[String,DomainContainer]( "d0" -> new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Lev,1,1) ) ) )
    new TaskRequest( "CWT.cache",  variableMap, domainMap )
  }

}

object executionTest extends App {
  val request = SampleTaskRequests.getAnomalyTimeseries
  val run_args = Map[String,Any]()
  val cds2ExecutionManager = new CDS2ExecutionManager()
  val result = cds2ExecutionManager.execute( request, run_args )
  println( result.toString )
}


//  TaskRequest: name= CWT.average, variableMap= Map(v0 -> DataContainer { id = hur:v0, dset = merra/mon/atmos, domain = d0 }, ivar#1 -> OperationContainer { id = ~ivar#1,  name = , result = ivar#1, inputs = List(v0), optargs = Map(axis -> xy) }), domainMap= Map(d0 -> DomainContainer { id = d0, axes = List(DomainAxis { id = lev, start = 0, end = 1, system = "indices", bounds =  }) })

