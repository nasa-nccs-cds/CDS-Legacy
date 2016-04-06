package nasa.nccs.cds2.modules.CDS

import nasa.nccs.cdapi.cdm.{BinnedArrayFactory, KernelDataInput, PartitionedFragment, aveSliceAccumulator}
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.Nd4jMaskedTensor
import nasa.nccs.cds2.kernels.KernelTools
import nasa.nccs.esgf.process.{ServerContext, DataSource, OperationContext, RequestContext}
import org.nd4j.linalg.api.ndarray.INDArray

import scala.reflect.runtime._
import scala.reflect.runtime.universe._

class CDS extends KernelModule with KernelTools {
  override val version = "1.0-SNAPSHOT"
  override val organization = "nasa.nccs"
  override val author = "Thomas Maxwell"
  override val contact = "thomas.maxwell@nasa.gov"

  class raw_average extends Kernel {    // For timing comparisons only- does not account for missing values, uses NDArray builtin mean operator.
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Raw Average over Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  inputVars( operationCx, requestCx, serverCx ).head
      val optargs: Map[String,String] =  operationCx.getConfiguration
      val input_array = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t0 = System.nanoTime
      val mean_val = input_array.rawmean( axes:_* )
      val t1 = System.nanoTime
      logger.info("Kernel %s: Executed operation %s, time= %.4f s, result = %s ".format(name, operation, (t1-t0)/1.0E9, mean_val.toString ))
      val variable = serverCx.getVariable( inputVar.getSpec )
      val variableMetadata = inputVar.getVariableMetadata( serverCx )
      val section = inputVar.getSpec.getReducedSection(Set(axes:_*))
      if( async ) {
        new AsyncExecutionResult( saveResult( mean_val, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), mean_val )
    }
  }

  class average extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Average over Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  inputVars( operationCx, requestCx, serverCx ).head
      val optargs: Map[String,String] =  operationCx.getConfiguration
      val input_array = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val mean_val_masked = input_array.mean( axes:_* )
      val t11 = System.nanoTime
      logger.info("Mean_val_masked, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, mean_val_masked.toString ) )
      val variable = serverCx.getVariable( inputVar.getSpec )
      val section = inputVar.getSpec.getReducedSection(Set(axes:_*))
      if(async) {
        new AsyncExecutionResult( saveResult( mean_val_masked, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), mean_val_masked )
    }
  }
  class subset extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Average over Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  inputVars( operationCx, requestCx, serverCx ).head
      val optargs: Map[String,String] =  operationCx.getConfiguration
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t0 = System.nanoTime
      def input_uids = requestCx.getDataSources.keySet
      assert( input_uids.size == 1, "Wrong number of arguments to 'subset': %d ".format(input_uids.size) )
      val result: PartitionedFragment = optargs.get("domain") match {
        case None => inputVar.dataFragment
        case Some(domain_id) => serverCx.getSubset( requestCx.getInputSpec(input_uids.head).data, requestCx.getDomain(domain_id) )
      }
      val t1 = System.nanoTime
      logger.info("Subset: time = %.4f s, result = %s, value = %s".format( (t1-t0)/1.0E9, result.toString, result.data.toRawDataString ) )
      val variable = serverCx.getVariable( inputVar.getSpec )
      val section = inputVar.getSpec.getSubSection(result.fragmentSpec.roi)
      if(async) {
        new AsyncExecutionResult( saveResult( result.data, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), result.data )
    }
  }

  class bin extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Binning over Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  inputVars( operationCx, requestCx, serverCx ).head
      val optargs: Map[String,String] =  operationCx.getConfiguration
      val input_array = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val binFactory: BinnedArrayFactory = serverCx.getBinnedArrayFactory(operationCx,requestCx) match {
        case None => throw new Exception( "Must include bin spec in bin operation")
        case Some(bf) => bf
      }
      assert( axes.length == 1, "Must bin over 1 axis only! Requested: " + axes.mkString(",") )
      val binned_value: Option[Nd4jMaskedTensor] = input_array.bin(axes.head,binFactory)
      val t11 = System.nanoTime
      binned_value match {
        case None => throw new Exception("Empty Bins");
        case Some(masked_array) =>
          logger.info("Binned array, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, masked_array.toRawDataString ) )
          val variable = serverCx.getVariable( inputVar.getSpec )
          val section = inputVar.getSpec.getReducedSection(Set(axes(0)),masked_array.shape(axes(0)))
          if (async) {
            new AsyncExecutionResult(saveResult(masked_array, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ))
          }
          else new BlockingExecutionResult(operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), masked_array )
      }
    }
  }

  class anomaly extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Anomaly over Input Fragment"

    def execute( operationCx: OperationContext, requestCx: RequestContext, serverCx: ServerContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  inputVars( operationCx, requestCx, serverCx ).head
      val optargs: Map[String,String] =  operationCx.getConfiguration
      val input_array = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisIndices
      val async = requestCx.config("async", "false").toBoolean
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val mean_val_masked = input_array.mean( axisSpecs.getAxes:_* )
      val bc_mean_val_masked = mean_val_masked.broadcast( input_array.shape:_* )
      val anomaly_result = input_array - bc_mean_val_masked
      val variable = serverCx.getVariable( inputVar.getSpec )
      val section = inputVar.getSpec.roi
      val t11 = System.nanoTime
      logger.info("Anomaly, time = %.4f s".format( (t11-t10)/1.0E9 ) )
      if(async) {
        new AsyncExecutionResult( saveResult( anomaly_result, requestCx, serverCx, variable.getGridSpec(section), inputVar.getVariableMetadata(serverCx), inputVar.getDatasetMetadata(serverCx) ) )
      }
      else new BlockingExecutionResult( operationCx.identifier, List(inputVar.getSpec), variable.getGridSpec(section), anomaly_result )
    }
  }
}

object arrayTest extends App {
  import org.nd4j.linalg.factory.Nd4j
  var data = Array(1.0.toFloat, 1.0.toFloat, Float.NaN )
  var arr2 = Nd4j.create( data )
  var fmean = arr2.mean(1)
  println( "Mean: "+ fmean.toString )
}
