package nasa.nccs.cds2.engine

import nasa.nccs.cds2.cdm.CDSVariable
import nasa.nccs.cds2.{kernels, cdm}
import nasa.nccs.cds2.loaders.Collections
import nasa.nccs.esgf.process._
import nasa.nccs.esgf.engine.PluginExecutionManager
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.slf4j.LoggerFactory
import scala.collection.mutable
import nasa.nccs.cds2.utilities.cdsutils

object cds2PluginExecutionManager extends PluginExecutionManager {
  val cds2ExecutionManager = new CDS2ExecutionManager()

  override def execute( process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], run_args: Map[String,Any] ): xml.Elem = {
    val request = TaskRequest( process_name, datainputs )
    cds2ExecutionManager.execute( request, run_args )
  }
}

class ExecutionResult( val result_data: Array[Float] ) {
  def toXml = <result> { result_data.mkString( " ", ",", " " ) } </result>  // cdsutils.cdata(
}

class ExecutionResults( val results: List[ExecutionResult] ) {
    def toXml = <execution> {  results.map(_.toXml )  } </execution>
}

class SingleInputExecutionResult( val operation: String, input: CDSVariable, result_data: Array[Float] ) extends ExecutionResult(result_data) {
  val name = input.name
  val description = input.description
  val units = input.units
  val dataset =  input.dataset.name

  override def toXml =
    <operation id={ operation }>
      <input name={ name } dataset={ dataset } units={ units } description={ description }  />
      { super.toXml }
    </operation>
}

class CDS2ExecutionManager {
  val logger = LoggerFactory.getLogger(classOf[CDS2ExecutionManager])

  def execute( request: TaskRequest, run_args: Map[String,Any] ): xml.Elem = {
    logger.info("Execute { request: " + request.toString + ", runargs: " + run_args.toString + "}"  )
    val data_manager = new DataManager( request.domainMap )
    for( data_container <- request.variableMap.values; if data_container.isSource )  data_manager.loadVariableData( data_container.uid, data_container.getSource )
    executeWorkflows( request.workflows, data_manager, run_args ).toXml
  }

  def executeWorkflows( workflows: List[WorkflowContainer], data_manager: DataManager, run_args: Map[String,Any] ): ExecutionResults = {
//    val kernelManager = new kernels.KernelManager()
    new ExecutionResults( workflows.map( workflow => workflow.operations.map( operation => demoOperationExecution( operation, data_manager,  run_args ) ).flatten ).flatten )
  }

  def demoOperationExecution(operation: OperationContainer, data_manager: DataManager, run_args: Map[String, Any]): List[ExecutionResult] = {
    val inputSubsets: List[cdm.Fragment] = operation.inputs.map(data_manager.getVariableData(_))
    inputSubsets.map(inputSubset => {
      new SingleInputExecutionResult( operation.name, inputSubset.variable,
        operation.name match {
          case "CWT.average" =>

            val result = Array[Float](Nd4j.mean(inputSubset.ndArray).getFloat(0))
            logger.info( "Executed operation %s, result = %s ".format( operation.name, result.mkString( "[", ",", "]" ) ) )
            result
          case _ => Array[Float]()
        })
    })
  }
}

class DataManager( val domainMap: Map[String,DomainContainer] ) {
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.engine.DataManager")
  var datasets = mutable.Map[String,cdm.CDSDataset]()
  var subsets = mutable.Map[String,cdm.Fragment]()

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

  def getVariableData(uid: String): cdm.Fragment = {
    subsets.get(uid) match {
      case Some(subset) => subset
      case None => throw new Exception("Can't find subset Data for Variable $uid")
    }
  }

  def loadVariableData(uid: String, data_source: DataSource): cdm.Fragment = {
    subsets.get(uid) match {
      case Some(subset) => subset
      case None =>
        val dataset: cdm.CDSDataset = getDataset(data_source)
        domainMap.get(data_source.domain) match {
          case Some(domain_container) =>
            val variable = dataset.loadVariable(data_source.name)
            val Fragment: cdm.Fragment = variable.loadRoi(domain_container.axes)
            subsets += uid -> Fragment
            logger.info("Loaded variable %s (%s:%s) subset data, shape = %s ".format(uid, data_source.collection, data_source.name, Fragment.shape.toString) )
            Fragment
          case None =>
            throw new Exception("Undefined domain for dataset " + data_source.name + ", domain = " + data_source.domain)
        }
    }
  }
}

object SampleTaskRequests {

  def getAnomalyTimeseries: TaskRequest = {
    import nasa.nccs.esgf.process.DomainAxis.Type._
    val workflows = List[WorkflowContainer]( new WorkflowContainer( operations = List( new OperationContainer( identifier = "CWT.average~ivar#1",  name ="CWT.average", result = "ivar#1", inputs = List("v0"), optargs = Map("axis" -> "xy") )  ) ) )
    val variableMap = Map[String,DataContainer]( "v0" -> new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) ) )
    val domainMap = Map[String,DomainContainer]( "d0" -> new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis(Lev,1,1), DomainAxis(Lat,100,100), DomainAxis(Lon,100,100) ) ) )
    new TaskRequest( "CWT.anomaly", variableMap, domainMap, workflows )
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

