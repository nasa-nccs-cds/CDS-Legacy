package nasa.nccs.cds2.kernels
import nasa.nccs.cds2.cdm
import nasa.nccs.cds2.engine.ExecutionResult
import org.slf4j.LoggerFactory

object Port {
  def apply( name: String, cardinality: String, description: String="", datatype: String="", identifier: String="" ) = {
    new Port(  name,  cardinality,  description, datatype,  identifier )
  }
}

class Port( val name: String, val cardinality: String, val description: String, val datatype: String, val identifier: String )  {

  def toXml = {
    <port name={name} cardinality={cardinality}>
      { if ( description.nonEmpty ) <description> {description} </description> }
      { if ( datatype.nonEmpty ) <datatype> {datatype} </datatype> }
      { if ( identifier.nonEmpty ) <identifier> {identifier} </identifier> }
    </port>
  }
}


abstract class Kernel( val inputs: List[Port], val outputs: List[Port], val description: String="", val keywords: List[String]=List(), val identifier: String="", val metadata:String="" ) {
  val logger = LoggerFactory.getLogger(classOf[Kernel])
  val identifiers = this.getClass.getName.split('$').flatMap( _.split('.') )
  def operation: String = identifiers.last
  def module = identifiers.dropRight(1).mkString(".")
  def id   = identifiers.mkString(".")
  def name = identifiers.takeRight(2).mkString(".")

  def execute(inputSubsets: List[cdm.Fragment], run_args: Map[String, Any]): ExecutionResult

  def toXml = {
    <kernel module={module} name={name}>
      {if (description.nonEmpty) <description>
      {description}
    </description>}{if (keywords.nonEmpty) <keywords>
      {keywords.mkString(",")}
    </keywords>}{if (identifier.nonEmpty) <identifier>
      {identifier}
    </identifier>}{if (metadata.nonEmpty) <metadata>
      {metadata}
    </metadata>}
    </kernel>
  }
}

class KernelModule {
  val identifiers = this.getClass.getName.split('$').flatMap( _.split('.') )
  def package_path = identifiers.dropRight(1).mkString(".")
  def name: String = identifiers.last
  val version = ""
  val organization = ""
  val author = ""
  val contact = ""

  def getKernelClasses = this.getClass.getClasses.toList.filter( _.getSuperclass.getName.split('.').last == "Kernel"  )

  def toXml = {

    <kernelModule name={name}>
      { if ( version.nonEmpty ) <description> {version} </description> }
      { if ( organization.nonEmpty ) <datatype> {organization} </datatype> }
      { if ( author.nonEmpty ) <identifier> {author} </identifier> }
      { if ( contact.nonEmpty ) <identifier> {contact} </identifier> }

    </kernelModule>
  }
}

