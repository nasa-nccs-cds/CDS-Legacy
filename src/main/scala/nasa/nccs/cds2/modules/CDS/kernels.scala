package nasa.nccs.cds2.modules.CDS

import nasa.nccs.cds2.cdm
import nasa.nccs.cds2.engine.{ExecutionResult, ExecutionResults}
import nasa.nccs.cds2.kernels.{ Kernel, Port, KernelModule }
import org.nd4j.linalg.factory.Nd4j

class CDS extends KernelModule {
  override val version = "1.0-SNAPSHOT"
  override val organization = "nasa.nccs"
  override val author = "Thomas Maxwell"
  override val contact = "thomas.maxwell@nasa.gov"

  class average extends Kernel(List(Port("input fragment", "1")), List(Port("result", "1")), "Average over Input Fragment") {
    def execute(inputSubsets: List[cdm.Fragment], run_args: Map[String, Any]): ExecutionResult = {
      val inputSubset = inputSubsets.head
      val result = Array[Float](Nd4j.mean(inputSubset.ndArray).getFloat(0))
      logger.info("Kernel %s: Executed operation %s, result = %s ".format(name, operation, result.mkString("[", ",", "]")))
      new ExecutionResult(Array.emptyFloatArray)
    }
  }

}

object modTest extends App {
  val cds = new CDS()
  cds.getKernels
}
