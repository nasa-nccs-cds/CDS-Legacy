package nasa.nccs.cds2.utilities
import ucar.nc2.time.CalendarDate
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.runtime.{universe=>ru}

object cdsutils {

  def flatlist[T]( values: Option[T]* ): List[T] = values.flatten.toList

  def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]

  def getInstance[T]( cls: Class[T] ) = cls.getConstructor().newInstance()

  def findNonNull[T]( values: T* ): Option[T] = values.toList.find( _ != null )

  def cdata(obj: Any): String = "<![CDATA[\n " + obj.toString + "\n]]>"

  object dateTimeParser {
    import com.joestelmach.natty
    private val parser = new natty.Parser()

    def parse(input: String): CalendarDate = {
      val caldates = mutable.ListBuffer[CalendarDate]()
      val groups = parser.parse(input).toList
      for (group: natty.DateGroup <- groups; date: java.util.Date <- group.getDates.toList) caldates += CalendarDate.of(date)
      assert( caldates.size == 1, " DateTime Parser Error: parsing '%s'".format(input) )
      caldates.head
    }
  }

  def getKernelPackages: Seq[String] = {
      import cdsbt.BuildInfo._
      kernelPackages
  }

//  def loadExtensionModule( jar_file: String, module: Class ): Unit = {
//    var classLoader = new java.net.URLClassLoader( Array(new java.io.File( jar_file ).toURI.toURL ), this.getClass.getClassLoader)
//    var clazzExModule = classLoader.loadClass(module.GetClass.GetName + "$") // the suffix "$" is for Scala "object",
//    try {
//      //"MODULE$" is a trick, and I'm not sure about "get(null)"
//      var module = clazzExModule.getField("MODULE$").get(null).asInstanceOf[module]
//    } catch {
//      case e: java.lang.ClassCastException =>
//        printf(" - %s is not Module\n", clazzExModule)
//    }
//
//  }
}

object dateParseTest extends App {
  val caldate:CalendarDate = cdsutils.dateTimeParser.parse( "10/10/1998 5:00 GMT")
  println( caldate.toString )
}




