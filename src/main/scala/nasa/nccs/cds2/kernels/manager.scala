package nasa.nccs.cds2.kernels


class KernelManager(  ) {

  val kernel_map: Map[String,Kernel] = collectDefinedKernels()

  def get( kernelName: String ): Option[Kernel] = kernel_map.get(kernelName)

  def collectDefinedKernels(): Map[String,Kernel] = {  Map[String,Kernel]() }

}



