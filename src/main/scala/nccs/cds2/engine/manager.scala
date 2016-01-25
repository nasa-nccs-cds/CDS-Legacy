package nccs.cds2.engine
import nccs.esgf.process._
import nccs.esgf.engine.PluginExecutionManager
import org.slf4j.LoggerFactory
import scala.collection.mutable
import nccs.cds2.utilities.cdsutils

object cds2PluginExecutionManager extends PluginExecutionManager {
  val cds2ExecutionManager = new CDS2ExecutionManager()

  override def execute( process_name: String, datainputs: Map[String, Seq[Map[String, Any]]], run_args: Map[String,Any] ): xml.Elem = {
    val request = TaskRequest( process_name, datainputs )
    cds2ExecutionManager.execute( request, run_args )
  }
}

class CDS2ExecutionManager {
  val logger = LoggerFactory.getLogger(classOf[CDS2ExecutionManager])

  def execute( request: TaskRequest, run_args: Map[String,Any] ): xml.Elem = {
    logger.info("Execute { request: " + request.toString + ", runargs: " + run_args.toString + "}"  )
    val data_manager = new DataManager( request.domainMap )
    for( data_container <- request.variableMap.values; if data_container.isSource )  data_manager.loadDataset( data_container.uid, data_container.getSource )
    request.toXml
  }
}

class DataManager( val domainMap: Map[String,DomainContainer] ) {
  var dataArrays = mutable.Map[String,ucar.ma2.Array]()

  def loadDataset( uid: String, data_source: DataSource ) = {
    import nccs.cds2.loaders.Collections
    import nccs.cds2.loaders.NetCDFReader
    domainMap.get(data_source.domain) match {
      case Some(domain_container) =>
        Collections.CreateIP.get( data_source.collection.toLowerCase ) match {
          case Some(collection) =>
            dataArrays += ( uid -> NetCDFReader.readArraySubset( data_source.name, collection,  domain_container.axes ) )
          case None =>
            throw new Exception( "Undefined collection for dataset " + data_source.name + ", collection = " + data_source.collection )
        }
      case None =>
        throw new Exception( "Undefined domain for dataset " + data_source.name + ", domain = " + data_source.domain )
    }
  }
}

object SampleTaskRequests {

  def getAnomalyTimeseries1: TaskRequest = {
    import nccs.esgf.process._

    val workflows = List[WorkflowContainer]( new WorkflowContainer( operations = List( new OperationContainer( identifier = "CWT.average~ivar#1",  name ="CWT.average", result = "ivar#1", inputs = List("v0"), optargs = Map("axis" -> "xy") )  ) ) )
    val variableMap = Map[String,DataContainer]( "v0" -> new DataContainer( uid="v0", source = Some(new DataSource( name = "hur", collection = "merra/mon/atmos", domain = "d0" ) ) ) )
    val domainMap = Map[String,DomainContainer]( "d0" -> new DomainContainer( name = "d0", axes = cdsutils.flatlist( DomainAxis("lev",0,1), DomainAxis("lat",100,101), DomainAxis("lon",100,101) ) ) )
    new TaskRequest( "CWT.anomaly",  workflows, variableMap, domainMap )
  }
}

object executionTest extends App {
  val request = SampleTaskRequests.getAnomalyTimeseries1
  val run_args = Map[String,Any]()
  val cds2ExecutionManager = new CDS2ExecutionManager()
  val result = cds2ExecutionManager.execute( request, run_args )
  println( result.toString )
}


//  TaskRequest: name= CWT.average, variableMap= Map(v0 -> DataContainer { id = hur:v0, dset = merra/mon/atmos, domain = d0 }, ivar#1 -> OperationContainer { id = ~ivar#1,  name = , result = ivar#1, inputs = List(v0), optargs = Map(axis -> xy) }), domainMap= Map(d0 -> DomainContainer { id = d0, axes = List(DomainAxis { id = lev, start = 0, end = 1, system = "indices", bounds =  }) })

