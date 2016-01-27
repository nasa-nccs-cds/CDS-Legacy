package nasa.nccs.cds2.loaders

object Collection { 
  def apply( ctype: String, url: String, vars: List[String] = List(), axes: Option[AxisNames] = None ) = { new Collection(ctype,url,vars,axes) }
}
class Collection( val ctype: String, val url: String, val vars: List[String] = List(), val axes: Option[AxisNames] ) {
  def getUrl( varName: String ) = {
    ctype match {
      case "dods" => s"$url/$varName.ncml"
      case _ => throw new Exception( s"Unrecognized collection type: $ctype")
    }
  }
}

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
  val CreateIP = Map(
    "merra/mon/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus" ), axes=AxisNames("lon","lat","plev","time") ),
    "cfsr/mon/atmos"  -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/mon/atmos",  vars=List( "va", "ta", "clt", "ua", "psl", "hus"  ), axes=AxisNames("lon","lat","plev","time") ),
    "ecmwf/mon/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/mon/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  ), axes=AxisNames("lon","lat","plev","time") ),
    "merra/6hr/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/6hr/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  ), axes=AxisNames("lon","lat","plev","time") ),
    "cfsr/6hr/atmos"  -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/6hr/atmos",  vars=List( "va", "ta", "clt", "ua", "psl", "hus"  ), axes=AxisNames("lon","lat","plev","time") ),
    "ecmwf/6hr/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/6hr/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  ), axes=AxisNames("lon","lat","plev","time") )
  )
}


