package nccs.engine
import nccs.process.TaskRequest

class ExecutionManager {}

object ExecutionManager {

  def execute( request: TaskRequest, run_args: Map[String,Any] ) = {
    SparkEngine.execute( request, run_args )
  }
}
