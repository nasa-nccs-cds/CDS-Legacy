package nasa.nccs.cds2.kernels
import nasa.nccs.cds2.cdm
import nasa.nccs.cds2.engine.ExecutionResults


abstract class Kernel( val inputs: List[Port], val outputs: List[Port], val description: Option[String]=None, val keywords: List[String]=List(), val identifier: Option[String]=None, val metadata: Option[String]=None ) {

    def execute( inputSubsets: List[cdm.Fragment], run_args: Map[String, Any] ): ExecutionResults
}

class Port( val name: String, val cardinality: String, val description: Option[String]=None, val datatype: Option[String]=None, val identifier: Option[String]=None )  {

}
