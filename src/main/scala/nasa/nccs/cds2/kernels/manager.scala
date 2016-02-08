package nasa.nccs.cds2.kernels


import collection.mutable

class KernelManager(  ) {

  val kernelModules = collectKernelModules()

  def getModule( moduleName: String ): Option[KernelModule] = kernelModules.get(moduleName)

  def toXml = <modules>{ kernelModules.values.map( _.toXml ) } </modules>

  def collectKernelModules(): Map[String,KernelModule] = {
    import nasa.nccs.cds2.modules.CDS.CDS
    import nasa.nccs.cds2.utilities.cdsutils
    val kernelModules = new mutable.ListBuffer[KernelModule]
//    import com.google.common.reflect.ClassPath
//    val classpath: ClassPath = ClassPath.from(classloader)
//    for( kernelPackageName: String <- cdsutils.getKernelPackages ) {
//      val classInfo = classpath.getTopLevelClasses(kernelPackageName)
//    }
    kernelModules += new CDS()
    Map( kernelModules.map( km => km.name -> km ):_* )
  }

}

object kernelManager extends KernelManager() { }



