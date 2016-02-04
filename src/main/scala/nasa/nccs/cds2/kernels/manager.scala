package nasa.nccs.cds2.kernels
import collection.mutable

class KernelManager(  ) {

  val kernel_map = collectDefinedKernels()

  def getKernel( kernelName: String ): Option[Kernel] = kernel_map.get(kernelName)

  def collectDefinedKernels(): mutable.HashMap[String,Kernel] = {  mutable.HashMap[String,Kernel]() }

  def addKernel( kernel: Kernel ) = {
    kernel_map += ( kernel.name.toLowerCase -> kernel )
  }

}

object kernelManager extends KernelManager() { }



