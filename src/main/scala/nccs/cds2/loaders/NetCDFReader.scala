package nccs.cds2.loaders

import nccs.cds2.utilities.NetCDFUtils
import nccs.esgf.process.DomainAxis
import org.slf4j.{LoggerFactory, Logger}
import ucar.nc2
import ucar.nc2.{ NetcdfFile, Variable }
import ucar.nc2.dataset.NetcdfDataset
import ucar.ma2
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Utility functions to create a multi-dimensional array from a NetCDF,
  * most importantly from some URI.
  */
object NetCDFReader {

  // Class logger
  val logger = LoggerFactory.getLogger("NetCDFReader")

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
      logger.warn("Dataset %s not found!".format(uri))
      return (Array(-9999), Array(1, 1))
    }

    val coordinateArray = NetCDFUtils.netCDFArrayAndShape(netcdfFile, variable)
    val variableArray = coordinateArray._1

    if (variableArray.length < 1) {
      logger.warn("Variable '%s' in dataset at %s not found!".format(variable, uri))
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
      logger.warn("Dataset %s not found!".format(variable) )
      return (Array(-9999), Array(1, 1))
    }

    val coordinateArray = NetCDFUtils.netCDFArrayAndShape(dataset, variable)
    val variableArray = coordinateArray._1

    if (variableArray.length < 1) {
      logger.warn("Variable '%s' in dataset not found!".format(variable))
      return (Array(-9999), Array(1, 1))
    }
    coordinateArray
  }

  def loadNetCDFFile(name: String, file: Array[Byte]): NetcdfDataset = {
    new NetcdfDataset(NetcdfFile.openInMemory(name, file))
  }
  def getDimensionIndex( ncVariable: Variable, cds_dim_name: String ): Option[Int] = {
    val dimension_names: List[String] = ncVariable.getDimensionsString.split(' ').map( _.toLowerCase).toList
    val dimension_name_opt: Option[String] = cds_dim_name match {
      case "lon" => dimension_names.find( ( name: String ) => name.startsWith("lon") || name.startsWith("x") )
      case "lat" => dimension_names.find( ( name: String ) => name.startsWith("lat") || name.startsWith("y") )
      case "lev" => dimension_names.find( ( name: String ) => name.startsWith("lev") || name.startsWith("plev") || name.startsWith("z") )
      case "time" => dimension_names.find( ( name: String ) => name.startsWith("t") )
    }
    dimension_name_opt match {
      case Some(dimension_name) => ncVariable.findDimensionIndex(dimension_name) match { case -1 => None; case x  => Some(x) }
      case _ => throw new Exception("Can't locate dimension $cds_dim_name in variable " + ncVariable.getNameAndDimensions(true) )
    }
  }

  def getSubset( ncVariable: Variable, collection : Collection, roi: List[DomainAxis] ): List[ma2.Range] = {
    val shape: mutable.ArrayBuffer[ma2.Range] = ncVariable.getRanges.to[mutable.ArrayBuffer]
    for( axis <- roi ) {
      val dim_index_opt: Option[Int] = collection.axes match {
        case None =>  getDimensionIndex( ncVariable, axis.name )
        case Some( axisNames ) =>
          axisNames( axis.dimension ) match {
            case Some(dimension) => Some( ncVariable.findDimensionIndex( dimension ) )
            case None => None
          }
      }
      dim_index_opt match {
        case Some(dim_index) => shape.update( dim_index, new ma2.Range( axis.start, axis.end, 1 ) )
        case None => None
      }
    }
    shape.toList
  }

  def getArrayShape( array: ucar.ma2.Array ): List[Int] = {
    var shape = mutable.ListBuffer[Int]()
    for( ival <- array.getShape ) shape += ival
    shape.toList
  }

  def readArraySubset( varName: String, collection : Collection,  roi: List[DomainAxis] ): ucar.ma2.Array = {
    val t0 = System.nanoTime
    val ncFile = NetCDFUtils.loadNetCDFDataSet( collection.getUrl( varName ) )
    val ncVariable = ncFile.findVariable(varName)
    if (ncVariable == null) throw new IllegalStateException("Variable '%s' was not loaded".format(varName))
    val subsetted_ranges = getSubset( ncVariable, collection, roi )
    val array = ncVariable.read( subsetted_ranges.asJava )
    val t1 = System.nanoTime
    val array_shape = getArrayShape( array )
    logger.info( "Read %s subset: %s, shape = %s, size = %d, time = %.4f s".format( varName, subsetted_ranges.toString, array_shape.toString, array.getSize, (t1-t0)/1e9 ) )
    array
  }

}

object testNetCDF extends App {
  import ucar.ma2._
  val url = "http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos/ta.ncml"
  val varName = "ta"
  val dset : NetcdfDataset = NetCDFUtils.loadNetCDFDataSet(url)
  val ncVariable = dset.findVariable(varName)
  if (ncVariable == null) throw new IllegalStateException( s"Variable $varName was not loaded" )
  println( ncVariable.getNameAndDimensions )
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

