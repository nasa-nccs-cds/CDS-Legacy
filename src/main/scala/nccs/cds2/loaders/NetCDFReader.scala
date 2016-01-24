package nccs.cds2.loaders

import nccs.cds2.utilities.NetCDFUtils
import nccs.esgf.process.DomainAxis
import org.slf4j.Logger
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import ucar.ma2.Section

/**
  * Utility functions to create a multi-dimensional array from a NetCDF,
  * most importantly from some URI.
  */
object NetCDFReader {

  // Class logger
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
    * Gets a multi-dimensional array from a NetCDF at some URI.
    *
    * @param uri where the NetCDF file is located
    * @param variable the NetCDF variable to extract
    * @return
    */
  def loadNetCDFNDVar(uri: String, variable: String): (Array[Double], Array[Int]) = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(uri)
    if (netcdfFile == null) {
      LOG.warn("Dataset %s not found!".format(uri))
      return (Array(-9999), Array(1, 1))
    }

    val coordinateArray = NetCDFUtils.netCDFArrayAndShape(netcdfFile, variable)
    val variableArray = coordinateArray._1

    if (variableArray.length < 1) {
      LOG.warn("Variable '%s' in dataset at %s not found!".format(variable, uri))
      return (Array(-9999), Array(1, 1))
    }
    coordinateArray
  }

  /**
    * Gets just the variable names of a NetCDF at some URI.
    */
  def loadNetCDFVar(uri: String): List[String] = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(uri)
    val vars = netcdfFile.getVariables
    /** We have to be mutable here because vars is a Java list which has no map function. */
    var list: List[String] = List()
    for (i <- 0 until vars.size) {
      val k = vars.get(i).getShortName
      list ++= List(k)
    }
    list
  }

  def loadNetCDFNDVar(dataset: NetcdfDataset, variable: String): (Array[Double], Array[Int]) = {
    if (dataset == null) {
      LOG.warn("Dataset %s not found!".format(variable) )
      return (Array(-9999), Array(1, 1))
    }

    val coordinateArray = NetCDFUtils.netCDFArrayAndShape(dataset, variable)
    val variableArray = coordinateArray._1

    if (variableArray.length < 1) {
      LOG.warn("Variable '%s' in dataset not found!".format(variable))
      return (Array(-9999), Array(1, 1))
    }
    coordinateArray
  }

  def loadNetCDFFile(name: String, file: Array[Byte]): NetcdfDataset = {
    new NetcdfDataset(NetcdfFile.openInMemory(name, file))
  }

  def getSubset( base_shape: Section, subset_axes: List[DomainAxis] ): Section = {  // TODO: complete getSubset
    new Section()
  }

  def readArraySubset( varName: String, collection : Collection,  axes: List[DomainAxis] ): String = {
    val ncFile = NetCDFUtils.loadNetCDFDataSet( collection.url )
    val ncVariable = ncFile.findVariable(varName)
    if (ncVariable == null) throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
    val subset_section: Section = getSubset( ncVariable.getShapeAsSection, axes )
    val result = ncVariable.read( subset_section )
    "test"                                       // TODO: return Array
  }

}

object testNetCDF extends App {
  import ucar.ma2._
  val url = "http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos/ta.ncml"
  val varName = "ta"
  val dset : NetcdfDataset = NetCDFUtils.loadNetCDFDataSet(url)
  val ncVariable = dset.findVariable(varName)
  if (ncVariable == null) throw new IllegalStateException( s"Variable $varName was not loaded" )
  println( ncVariable.getNameAndDimensions() )
  val shape: Section = new Section( ncVariable.getShapeAsSection )
  println( "Full Shape: " + shape.toString + " is Immutable: " + shape.isImmutable )
  val levelRange = new Range( 10, 10 )
  val latRange = new Range( 30, 30 )
  val lonRange = new Range( 100, 100 )
  shape.setRange( 1, levelRange )
  shape.setRange( 2, latRange )
  shape.setRange( 3, lonRange )
  val data_array = ncVariable.read( shape )
  println( "Subsetted Shape: " + shape.toString + ", Array Size: " + data_array.getSize )
}

