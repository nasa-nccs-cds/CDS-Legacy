package nasa.nccs.cds2.kernels

import nasa.nccs.cds2.utilities.cdsutils

import collection.mutable

class KernelManager(  ) {

  val kernel_map = collectDefinedKernels()

  def getKernel( kernelName: String ): Option[Kernel] = kernel_map.get(kernelName)

  def collectDefinedKernels(): mutable.HashMap[String,Kernel] = {
//    import com.google.common.reflect.ClassPath
//    val classpath: ClassPath = ClassPath.from(classloader)
//    for( kernelPackageName: String <- cdsutils.getKernelPackages ) {
//      val classInfo = classpath.getTopLevelClasses(kernelPackageName)
//    }
    new mutable.HashMap[String,Kernel]()
  }

  def addKernel( kernel: Kernel ) = {
    kernel_map += ( kernel.name.toLowerCase -> kernel )
  }

}

object kernelManager extends KernelManager() { }



