package nasa.nccs.cds2.kernels
import nasa.nccs.cdapi.kernels.KernelModule
import collection.mutable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class KernelMgr(  ) {

  val kernelModules = collectKernelModules()

  def getModule( moduleName: String ): Option[KernelModule] = kernelModules.get(moduleName)

  def toXml = <modules>{ kernelModules.values.map( _.toXml ) } </modules>

  def collectKernelModules(): Map[String,KernelModule] = {
    import nasa.nccs.cds2.modules.CDS.CDS, java.io.File, java.net.{URL, URLClassLoader}, java.util.jar.{ JarFile, JarEntry }
    val kernelModules = new mutable.ListBuffer[KernelModule]
    kernelModules += new CDS()
    val cpitems = System.getProperty("java.class.path").split( File.pathSeparator )
    for( cpitem <- cpitems; fileitem = new File(cpitem); if ( fileitem.isFile  && fileitem.getName.toLowerCase.endsWith(".jar") ); jarFile = new JarFile( fileitem ) ) {
      Option(jarFile.getManifest) match {
        case Some(manifest) =>
          if (manifest.getMainAttributes.getValue("Specification-Title") == "CDS2KernelModule") {
            val cloader: URLClassLoader  = URLClassLoader.newInstance( Array( new URL( "jar:file:" + fileitem+"!/" ) ) )
            for (je:JarEntry <- jarFile.entries; ename = je.getName; if ename.endsWith(".class"); cls = cloader.loadClass( ename.substring(0,ename.length-6).replace('/', '.') ) ) {
              if( cls.getSuperclass.getName ==  "nasa.nccs.cdapi.kernels.KernelModule" ) {
                kernelModules += cls.getDeclaredConstructors()(0).newInstance().asInstanceOf[KernelModule]
              }
            }
          }
        case x => Unit
      }
    }
    Map( kernelModules.map( km => km.name -> km ):_* )
  }
}

object kernelManager extends KernelMgr() {}


object kernelManagerTest extends App {
  println( kernelManager.toXml )
}

