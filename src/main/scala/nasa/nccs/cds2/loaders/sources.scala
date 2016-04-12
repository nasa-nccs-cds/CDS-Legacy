package nasa.nccs.cds2.loaders
import nasa.nccs.cdapi.cdm.Collection

object AxisNames {
  def apply( x: String = "", y: String = "", z: String = "", t: String = "" ): Option[AxisNames] = {
    val nameMap = Map( 'x' -> x, 'y' -> y, 'z' -> z, 't' -> t )
    Some( new AxisNames( nameMap ) )
  }
}
class AxisNames( val nameMap: Map[Char,String]  ) {
  def apply( dimension: Char  ): Option[String] = nameMap.get( dimension ) match {
    case Some(name) => if (name.isEmpty) None else Some(name)
    case None=> throw new Exception( s"Not an axis: $dimension" )
  }
}

object Collections {
  val datasets = Map(
    "merra/mon/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "cfsr/mon/atmos"  -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/mon/atmos",  vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "ecmwf/mon/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/mon/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "merra/6hr/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/6hr/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "cfsr/6hr/atmos"  -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/6hr/atmos",  vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  ),
    "ecmwf/6hr/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/6hr/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  )  )
  )
  def toXml(): xml.Elem = {
    <collections> { datasets.keys.map( id => <collection id={id} /> ) } </collections>
  }
  def toXml( collectionId: String ): xml.Elem = {
    datasets.get( collectionId ) match {
      case Some(collection) => <collection id={collectionId}> { collection.vars.mkString(",") } </collection>
      case None => <error> { "Invalid collection id:" + collectionId } </error>
    }
  }
}


