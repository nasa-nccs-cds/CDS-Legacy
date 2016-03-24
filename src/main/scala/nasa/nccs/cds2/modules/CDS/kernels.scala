package nasa.nccs.cds2.modules.CDS

import nasa.nccs.cdapi.cdm.{KernelDataInput, BinnedArrayFactory, aveSliceAccumulator, PartitionedFragment}
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cdapi.tensors.Nd4jMaskedTensor
import nasa.nccs.cds2.kernels.KernelTools
import nasa.nccs.esgf.process.DataSource
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

    def execute( context: ExecutionContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  context.fragments.head
      val optargs: Map[String,String] =  context.args
      val input_array = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisSpecs
      val axes = axisSpecs.getAxes
      val t0 = System.nanoTime
      val mean_val = input_array.rawmean( axes:_* )
      val t1 = System.nanoTime
      logger.info("Kernel %s: Executed operation %s, time= %.4f s, result = %s ".format(name, operation, (t1-t0)/1.0E9, mean_val.toString ))
      if(context.async) {
        new AsyncExecutionResult( saveResult( mean_val, context, inputVar.getVariableMetadata(context.dataManager), inputVar.getDatasetMetadata(context.dataManager) ) )
      }
      else new BlockingExecutionResult( mean_val.data )
    }
  }

  class average extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Average over Input Fragment"

    def execute(context: ExecutionContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  context.fragments.head
      val optargs: Map[String,String] =  context.args
      val input_array = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisSpecs
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val mean_val_masked = input_array.mean( axes:_* )
      val t11 = System.nanoTime
      println("Mean_val_masked, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, mean_val_masked.toString ) )
      if(context.async) {
        new AsyncExecutionResult( saveResult( mean_val_masked, context, inputVar.getVariableMetadata(context.dataManager), inputVar.getDatasetMetadata(context.dataManager) ) )
      }
      else new BlockingExecutionResult( mean_val_masked.data )
    }
  }
  class subset extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Average over Input Fragment"

    def execute(context: ExecutionContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  context.fragments.head
      val optargs: Map[String,String] =  context.args
      val input_array = inputVar.dataFragment
      val axisSpecs = inputVar.axisSpecs
      val axes = axisSpecs.getAxes
      val t0 = System.nanoTime
      def input_uids = context.getDataSources.keySet
      assert( input_uids.size == 1, "Wrong number of arguments to 'subset': %d ".format(input_uids.size) )
      val result: PartitionedFragment = context.args.get("domain") match {
        case None => input_array
        case Some(domain_id) => context.dataManager.getSubset( input_uids.head, context.getFragmentSpec(input_uids.head), context.getDomain(domain_id) )
      }
      val t1 = System.nanoTime
      println("Subset: time = %.4f s, result = %s, value = [ %s ]".format( (t1-t0)/1.0E9, result.toString, result.data.data.mkString(",") ) )
      if(context.async) {
        new AsyncExecutionResult( saveResult( result.data, context, inputVar.getVariableMetadata(context.dataManager), inputVar.getDatasetMetadata(context.dataManager) ) )
      }
      else new BlockingExecutionResult( result.data.data )
    }
  }

  class bin extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Binning over Input Fragment"

    def execute( context: ExecutionContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  context.fragments.head
      val optargs: Map[String,String] =  context.args
      val input_array: Nd4jMaskedTensor = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisSpecs
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val binFactory: BinnedArrayFactory = context.binArrayOpt match {
        case None => throw new Exception( "Must include bin spec in bin operation")
        case Some(bf) => bf
      }
      assert( axes.length == 1, "Must bin over 1 axis only! Requested: " + axes.mkString(",") )
      val binned_value: Option[Nd4jMaskedTensor] = input_array.bin(axes.head,binFactory)
      val t11 = System.nanoTime
      println("Binned array, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, binned_value.toString ) )
      binned_value match {
        case None => throw new Exception("Empty Bins");
        case Some(masked_array) =>
          if (context.async) {
            new AsyncExecutionResult(saveResult(masked_array, context, inputVar.getVariableMetadata(context.dataManager), inputVar.getDatasetMetadata(context.dataManager) ))
          }
          else new BlockingExecutionResult(masked_array.data)
      }
    }
  }

  class anomaly extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Anomaly over Input Fragment"

    def execute(context: ExecutionContext ): ExecutionResult = {
      val inputVar: KernelDataInput  =  context.fragments.head
      val optargs: Map[String,String] =  context.args
      val input_array = inputVar.dataFragment.data
      val axisSpecs = inputVar.axisSpecs
      val axes = axisSpecs.getAxes
      val t10 = System.nanoTime
      val mean_val_masked = input_array.mean( axisSpecs.getAxes:_* )
      val bc_mean_val_masked = mean_val_masked.broadcast( input_array.shape:_* )
      val anomaly_result = input_array - bc_mean_val_masked
      val t11 = System.nanoTime
      println("Anomaly, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, anomaly_result.toString ) )
      if(context.async) {
        new AsyncExecutionResult( saveResult( anomaly_result, context, inputVar.getVariableMetadata(context.dataManager), inputVar.getDatasetMetadata(context.dataManager) ) )
      }
      else new BlockingExecutionResult( context.id, List(inputVar.getSpec), anomaly_result.data )
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
