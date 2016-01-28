package nasa.nccs.cds2.cdm

import nasa.nccs.cds2.cdm
import nasa.nccs.cds2.utilities.cdsutils
import org.nd4j.linalg.api.ndarray.INDArray
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
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.cdm.dataset")

  def load( collection: Collection, varName: String = "" ) = {
    val ncDataset: NetcdfDataset = loadNetCDFDataSet( collection.getUrl( varName ) )
    val coordSystems: List[CoordinateSystem] = ncDataset.getCoordinateSystems.toList
    assert( coordSystems.size <= 1, "Multiple coordinate systems for one dataset is not supported" )
    if(coordSystems.isEmpty) throw new IllegalStateException("Error creating coordinate system for variable " + varName )
    new CDSDataset( ncDataset, coordSystems.head )
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

class CDSDataset(val ncDataset: NetcdfDataset, val coordSystem: CoordinateSystem ) {
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
  def getCoordinateAxis( dimension: Char ): Option[CoordinateAxis] = {
    dimension match {
      case 'x' => cdsutils.findNonNull( coordSystem.getLonAxis, coordSystem.getXaxis )
      case 'y' => cdsutils.findNonNull( coordSystem.getLatAxis, coordSystem.getYaxis )
      case 'z' => cdsutils.findNonNull( coordSystem.getPressureAxis, coordSystem.getHeightAxis )
      case 't' => cdsutils.findNonNull( coordSystem.getTaxis )
    }
  }
}
