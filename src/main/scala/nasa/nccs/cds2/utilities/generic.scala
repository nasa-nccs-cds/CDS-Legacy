package nasa.nccs.cds2.utilities
import ucar.nc2.time.CalendarDate
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object cdsutils {

  def flatlist[T]( values: Option[T]* ): List[T] = values.flatten.toList

  def findNonNull[T]( values: T* ): Option[T] = values.toList.find( _ != null )

  def toOption[T]( value: T ): Option[T] = if( value == null ) None else Some(value)

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
}

object dateParseTest extends App {
  val caldate:CalendarDate = cdsutils.dateTimeParser.parse( "10/10/1998 5:00 GMT")
  println( caldate.toString )
}




