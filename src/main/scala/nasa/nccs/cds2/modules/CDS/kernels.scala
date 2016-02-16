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

    class AveOp {
      var value_sum  = 0.toFloat
      var value_count = 0
      def init = { value_sum  = 0.toFloat; value_count = 0 }
      def addItem( value: Float ) = { value_sum += value; value_count += 1 }
      def getResult = value_sum / value_count
    }

    def applyOp( input: INDArray, missing_value: Float, op: AveOp ): Float = {
      op.init
      for( iC <- 0 until input.length )  {
        val v = input.getFloat(iC)
        if( v != missing_value ) op.addItem(v)
      }
      op.getResult
    }

    def getRow( input: INDArray, iR: Int ): INDArray = {
      val row_data = input.slice(iR,0)
      row_data.linearView
    }

    def execute(inputSubsets: List[DataFragment], run_args: Map[String, Any]): ExecutionResult = {
      val input_array = getNdArray(inputSubsets)
      val t0 = System.nanoTime
      val mean_val =input_array._1.mean(0)
      val t1 = System.nanoTime
      val result = mean_val.data.asFloat
      logger.info("Kernel %s: Executed operation %s, time= %.4f s, result = %s ".format(name, operation, (t1-t0)/1.0E9, result.mkString("[", ",", "]")))
      val t10 = System.nanoTime
      val reshaped_input_array = input_array._1.reshape( input_array._1.shape()(0), input_array._1.shape()(3) )
      val nRows = reshaped_input_array.shape()(1)
      val op = new AveOp()
      val mean_val_masked = ( 0 until nRows ).map( iR => applyOp( reshaped_input_array.slice(iR,0).ravel, input_array._2, op ) ) // getRow( reshaped_input_array, iR ) ) )
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
