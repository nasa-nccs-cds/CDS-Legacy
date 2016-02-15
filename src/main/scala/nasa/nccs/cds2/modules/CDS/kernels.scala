package nasa.nccs.cds2.modules
import nasa.nccs.cdapi.kernels.{ Kernel, Port, KernelModule, ExecutionResult, DataFragment }
import nasa.nccs.cds2.kernels.KernelTools
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

    def execute(inputSubsets: List[DataFragment], run_args: Map[String, Any]): ExecutionResult = {
      val input_array = getNdArray(inputSubsets)
      val t0 = System.nanoTime
      val mean_val =input_array.mean(0)
      val t1 = System.nanoTime
      val result = mean_val.data.asFloat
      logger.info("Kernel %s: Executed operation %s, time= %.4f s, result = %s ".format(name, operation, (t1-t0)/10.0e9, result.mkString("[", ",", "]")))
      val new_sum = 0.0
      val reshaped_input_array = input_array.reshape( input_array.shape()(0), input_array.shape()(3) )
      for( iR <- 0 to reshaped_input_array.shape()(1) ) {
        val col = reshaped_input_array.getColumn(0)
        println('.')
      }

      new ExecutionResult(Array.emptyFloatArray)
    }
  }
}


object arrayTest extends App {
  var data = Array(1.0.toFloat, 1.0.toFloat, Float.NaN )
  var arr2 = Nd4j.create( data )
  var fmean = arr2.mean(1)
  println( "Mean: "+ fmean.toString )
}
