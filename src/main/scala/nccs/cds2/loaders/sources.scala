package nccs.cds2.loaders

object Collection { 
  def apply( ctype: String, url: String, vars: List[String] = List() ) = { new Collection(ctype,url,vars) }
}
class Collection( val ctype: String, val url: String, val vars: List[String] = List() ) {
  def getUrl( varName: String ) = {
    ctype match {
      case "dods" => s"$url/$varName.ncml"
      case _ => throw new Exception( "Unrecognized collection type: $ctype")
    }
  }
}

object Collections {
  val CreateIP = Map(
    "merra/mon/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus" ) ),
    "cfsr/mon/atmos"  -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/mon/atmos",  vars=List( "va", "ta", "clt", "ua", "psl", "hus"  ) ),
    "ecmwf/mon/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/mon/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  ) ),
    "merra/6hr/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/6hr/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  ) ),
    "cfsr/6hr/atmos"  -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/6hr/atmos",  vars=List( "va", "ta", "clt", "ua", "psl", "hus"  ) ),
    "ecmwf/6hr/atmos" -> Collection( ctype="dods", url="http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/6hr/atmos", vars=List( "va", "ta", "clt", "ua", "psl", "hus"  ) )
  )
}


