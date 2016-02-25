package nasa.nccs.cds2.modules.CDS

import nasa.nccs.cdapi.cdm.PartitionedFragment
import nasa.nccs.cdapi.kernels._
import nasa.nccs.cds2.kernels.KernelTools
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4s.Implicits._

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
      val inputSubsets: List[PartitionedFragment] =  context.fragments
      val optargs: Map[String,String] =  context.args
      val input_array = inputSubsets(0).data
      val axisSpecs = inputSubsets(0).axisSpecs
      val axes = axisSpecs.getAxes
      val t0 = System.nanoTime
      val mean_val = input_array.rawmean( axes:_* )
      val t1 = System.nanoTime
      logger.info("Kernel %s: Executed operation %s, time= %.4f s, result = %s ".format(name, operation, (t1-t0)/1.0E9, mean_val.toString ))
      new ExecutionResult( mean_val.data )
    }
  }

  class average extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Average over Input Fragment"

    def execute(context: ExecutionContext ): ExecutionResult = {
      val input_array = context.fragments(0).data
      val axes = context.fragments(0).axisSpecs.getAxes
      val t10 = System.nanoTime
      val mean_val_masked = context.bins match {
        case None => input_array.mean( axes:_* )
        case Some(binsArray) => input_array.mean( axes:_*, binsArray )
      }
      val t11 = System.nanoTime
      println("Mean_val_masked, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, mean_val_masked.toString ) )
      new ExecutionResult( mean_val_masked.data )
    }
  }

  class anomaly extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Anomaly over Input Fragment"

    def execute(context: ExecutionContext ): ExecutionResult = {
      val inputSubsets: List[PartitionedFragment] =  context.fragments
      val optargs: Map[String,String] =  context.args
      val input_array = inputSubsets(0).data
      val axisSpecs = inputSubsets(0).axisSpecs
      val t10 = System.nanoTime
      val mean_val_masked = input_array.mean( axisSpecs.getAxes:_* )
      val bc_mean_val_masked = mean_val_masked.broadcast( input_array.shape:_* )
      val anomaly_result = input_array - bc_mean_val_masked
      val t11 = System.nanoTime
      println("Anomaly, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, anomaly_result.toString ) )
      new ExecutionResult( anomaly_result.data )
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
