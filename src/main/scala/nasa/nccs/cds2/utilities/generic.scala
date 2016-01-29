package nasa.nccs.cds2.utilities

import ucar.nc2.time.CalendarDate


object cdsutils {
  import com.joestelmach.natty

  def flatlist[T]( values: Option[T]* ): List[T] = values.flatten.toList

  def findNonNull[T]( values: T* ): Option[T] = values.toList.find( _ != null )

  def parseDate( input: String ): CalendarDate = {
    val parser = new natty.Parser()
    val groups = parser.parse(input)
    for( group <- groups ) {
      println(".") //val dates = group.toString // getDates
    }
    new CalendarDate()
  }
}

object dateParseTest extends App {
    val value = cdsutils.parseDate( "10/10/1998 5:00")
}




