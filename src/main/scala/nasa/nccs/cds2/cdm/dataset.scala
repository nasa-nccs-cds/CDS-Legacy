package nasa.nccs.cds2.cdm

import nasa.nccs.cds2.cdm
import nasa.nccs.cds2.utilities.cdsutils
import nasa.nccs.esgf.process.DomainAxis
import org.nd4s.Implicits._
import ucar.nc2.constants.AxisType
import ucar.{ma2, nc2}
import ucar.nc2.Variable
import ucar.nc2.dataset.{ NetcdfDataset, CoordinateSystem, CoordinateAxis }
import nasa.nccs.cds2.loaders.Collection
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by tpmaxwel on 1/28/16.
  */

object CDSDataset {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def load( dsetName: String, collection: Collection, varName: String = "" ) = {
    val uri = collection.getUri( varName )
    val ncDataset: NetcdfDataset = loadNetCDFDataSet( uri )
    val coordSystems: List[CoordinateSystem] = ncDataset.getCoordinateSystems.toList
    assert( coordSystems.size <= 1, "Multiple coordinate systems for one dataset is not supported" )
    if(coordSystems.isEmpty) throw new IllegalStateException("Error creating coordinate system for variable " + varName )
    new CDSDataset( dsetName, uri, ncDataset, coordSystems.head )
  }

  private def loadNetCDFDataSet(url: String): NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    try {
      NetcdfDataset.openDataset(url)
    } catch {
      case e: java.io.IOException =>
        logger.error("Couldn't open dataset %s".format(url))
        throw e
      case ex: Exception =>
        logger.error("Something went wrong while reading %s".format(url))
        throw ex
    }
  }
}

class CDSDataset( val name: String, val uri: String, val ncDataset: NetcdfDataset, val coordSystem: CoordinateSystem ) {
  val attributes = ncDataset.getGlobalAttributes
  val coordAxes: List[CoordinateAxis] = ncDataset.getCoordinateAxes.toList
  val variables = mutable.HashMap.empty[String,cdm.CDSVariable]

  def loadVariable( varName: String ): cdm.CDSVariable = {
    variables.get(varName) match {
      case None =>
        val ncVariable = ncDataset.findVariable(varName)
        if (ncVariable == null) throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
        val cdsVariable = new cdm.CDSVariable( varName, this, ncVariable )
        variables += ( varName -> cdsVariable )
        cdsVariable
      case Some(cdsVariable) => cdsVariable
    }
  }
  def getVariable( varName: String ): cdm.CDSVariable = {
    variables.get(varName) match {
      case None => throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
      case Some(cdsVariable) => cdsVariable
    }
  }
  def getCoordinateAxis( axisType: DomainAxis.Type.Value ): Option[CoordinateAxis] = {
    axisType match {
      case DomainAxis.Type.X => Option( coordSystem.getXaxis )
      case DomainAxis.Type.Y => Option( coordSystem.getYaxis )
      case DomainAxis.Type.Z => Option( coordSystem.getHeightAxis )
      case DomainAxis.Type.Lon => Option( coordSystem.getLonAxis )
      case DomainAxis.Type.Lat => Option( coordSystem.getLatAxis )
      case DomainAxis.Type.Lev => Option( coordSystem.getPressureAxis )
      case DomainAxis.Type.T => Option( coordSystem.getTaxis )
    }
  }
  def getCoordinateAxis( axisClass: Char ): Option[CoordinateAxis] = {
    axisClass.toLower match {
      case 'x' => if( coordSystem.isGeoXY ) Option( coordSystem.getXaxis ) else Option( coordSystem.getLonAxis )
      case 'y' => if( coordSystem.isGeoXY ) Option( coordSystem.getYaxis ) else Option( coordSystem.getLatAxis )
      case 'z' =>
        if( coordSystem.containsAxisType( AxisType.Pressure ) ) Option( coordSystem.getPressureAxis )
        else if( coordSystem.containsAxisType( AxisType.Height ) ) Option( coordSystem.getHeightAxis ) else  Option( coordSystem.getZaxis )
      case 't' => Option( coordSystem.getTaxis )
    }
  }
}
