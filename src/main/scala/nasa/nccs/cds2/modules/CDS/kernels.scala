package nasa.nccs.cds2.modules.CDS
import nasa.nccs.cdapi.kernels.{ Kernel, Port, KernelModule, ExecutionResult, DataFragment }
import nasa.nccs.cds2.kernels.KernelTools
import org.nd4j.linalg.factory.Nd4j

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
      val result = Array[Float](Nd4j.mean(input_array).getFloat(0))
      logger.info("Kernel %s: Executed operation %s, result = %s ".format(name, operation, result.mkString("[", ",", "]")))
      new ExecutionResult(Array.emptyFloatArray)
    }
  }
}
