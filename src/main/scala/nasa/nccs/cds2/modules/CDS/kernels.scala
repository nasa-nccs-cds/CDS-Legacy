package nasa.nccs.cds2.modules.CDS
import nasa.nccs.cdapi.kernels.{ Kernel, Port, KernelModule, ExecutionResult, DataFragment }
import nasa.nccs.cds2.kernels.KernelTools
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4s.Implicits._

class CDS extends KernelModule with KernelTools {
  override val version = "1.0-SNAPSHOT"
  override val organization = "nasa.nccs"
  override val author = "Thomas Maxwell"
  override val contact = "thomas.maxwell@nasa.gov"

  class average extends Kernel {
    val inputs = List(Port("input fragment", "1"))
    val outputs = List(Port("result", "1"))
    override val description = "Average over Input Fragment"

    def getRow( input: INDArray, iR: Int ): INDArray = {
      val row_data = input.slice(iR,0)
      row_data.linearView
    }

    def execute(inputSubsets: List[DataFragment], run_args: Map[String, Any]): ExecutionResult = {
      val input_array = getNdArray( inputSubsets, 0 )
      val t0 = System.nanoTime
      val mean_val = input_array.rawmean(0)
      val t1 = System.nanoTime
      logger.info("Kernel %s: Executed operation %s, time= %.4f s, result = %s ".format(name, operation, (t1-t0)/1.0E9, mean_val.toString ))
      val t10 = System.nanoTime
      val mean_val_masked = input_array.mean(0)
      val t11 = System.nanoTime
      println("Mean_val_masked, time = %.4f s, result = %s".format( (t11-t10)/1.0E9, mean_val_masked.toString ) )
      new ExecutionResult(Array.emptyFloatArray)
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
