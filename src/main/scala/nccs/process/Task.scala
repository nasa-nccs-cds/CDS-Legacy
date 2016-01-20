package nccs.process

import scala.util.matching.Regex
import scala.collection.mutable
import scala.xml._
import mutable.ListBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import nccs.process.exceptions._

case class ErrorReport(severity: String, message: String) {
  override def toString() = {
    s"ErrorReport { severity: $severity, message: $message }"
  }

  def toXml() = {
      <error severity={severity} message={message}/>
  }
}

class TaskRequest(val name: String,  val workflows: List[WorkflowContainer], val variableMap : Map[String,Any], val domainMap: Map[String,DomainContainer] ) {
  val errorReports = new ListBuffer[ErrorReport]()
  val logger = LoggerFactory.getLogger( classOf[TaskRequest] )
  validate()

  def addErrorReport(severity: String, message: String) = {
    val error_rep = ErrorReport(severity, message)
    logger.error(error_rep.toString)
    errorReports += error_rep
  }

  def validate(): Unit = {
    for( variable <- inputVariables; domid = variable.domain; vid=variable.id; if !domid.isEmpty ) {
      if ( !domainMap.contains(domid) ) {
        var keylist = domainMap.keys.mkString("[",",","]")
        logger.error( s"Error, No $domid in $keylist in variable $vid" )
        throw new NotAcceptableException( s"Error, Missing domain $domid in variable $vid" )
      }
    }
    for (workflow <- workflows; operation <- workflow.operations; opid = operation.id; var_arg <- operation.inputs) {
      if (!variableMap.contains(var_arg)) {
        var keylist = variableMap.keys.mkString("[", ",", "]")
        logger.error(s"Error, No $var_arg in $keylist in operation $opid")
        throw new NotAcceptableException(s"Error, Missing variable $var_arg in operation $opid")
      }
    }
  }

  override def toString = {
    var taskStr = s"TaskRequest { name='$name', variables = '$variableMap', domains='$domainMap', workflows='$workflows' }"
    if (!errorReports.isEmpty) {
      taskStr += errorReports.mkString("\nError Reports: {\n\t", "\n\t", "\n}")
    }
    taskStr
  }

  def toXml() = {
    <task_request name={name}>
      <data>
        { inputVariables.map(_.toXml()) }
      </data>
      <domains>
        {domainMap.values.map(_.toXml())}
      </domains>
      <operation>
        { workflows.map(_.toXml()) }
      </operation>
      <error_reports>
        {errorReports.map(_.toXml())}
      </error_reports>
    </task_request>
  }

  def inputVariables(): Traversable[DataContainer] = {
    for( variableSource <- variableMap.values; if variableSource.isInstanceOf[ DataContainer ] ) yield variableSource.asInstanceOf[DataContainer]
  }
}

object TaskRequest {
  def apply(process_name: String, datainputs: Map[String, Seq[Map[String, Any]]]) = {
    val data_list = datainputs.getOrElse("variable", List()).map(DataContainer(_)).toList
    val domain_list = datainputs.getOrElse("domain", List()).map(DomainContainer(_)).toList
    val operation_list = datainputs.getOrElse("operation", List()).map(WorkflowContainer(_)).toList
    val variableMap = buildVarMap( data_list, operation_list )
    val domainMap = buildDomainMap( domain_list )
    new TaskRequest( process_name, operation_list, variableMap, domainMap )
  }

  def buildVarMap( data: List[DataContainer], workflow: List[WorkflowContainer] ): Map[String,Any] = {
    var var_items = new ListBuffer[(String,Any)]()
    for( data_container <- data ) var_items += ( data_container.uid -> data_container )
    for( workflow_container<- workflow; operation<-workflow_container.operations; if !operation.result.isEmpty ) var_items += ( operation.result -> operation )
    var_items.toMap[String,Any]
  }

  def buildDomainMap( domain: List[DomainContainer] ): Map[String,DomainContainer] = {
    var domain_items = new ListBuffer[(String,DomainContainer)]()
    for( domain_container <- domain ) domain_items += ( domain_container.id -> domain_container )
    domain_items.toMap[String,DomainContainer]
  }
}




class TaskProcessor {

}
