package nasa.nccs.cds2.modules.CDS

import nasa.nccs.cds2.cdm
import nasa.nccs.cds2.engine.{ExecutionResult, ExecutionResults}
import nasa.nccs.cds2.kernels.{ Kernel, Port }
import org.nd4j.linalg.factory.Nd4j

object average extends Kernel( List( Port("input fragment","1") ), List( Port("result","1") ), "Average over Input Fragment" )  {
  def execute( inputSubsets: List[cdm.Fragment], run_args: Map[String, Any] ): ExecutionResult = {
    val inputSubset = inputSubsets(0)
    val result = Array[Float](Nd4j.mean(inputSubset.ndArray).getFloat(0))
    logger.info( "Kernel %s: Executed operation %s, result = %s ".format( name, operation, result.mkString( "[", ",", "]" ) ) )
    new ExecutionResult( Array.emptyFloatArray )
  }
}

object modTest extends App {
  println( average.module )
  println( average.name )
}
