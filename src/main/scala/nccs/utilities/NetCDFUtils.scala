package nccs.utilities

import org.slf4j.Logger
import ucar.ma2
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import scala.language.implicitConversions

/**
  * Utilities to read a NetCDF by URL or from HDFS.
  *
  * Note that we use -9999.0 instead of NaN to indicate missing values.
  */
object NetCDFUtils {

  // Class logger
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
    * Extracts a variable's data from a NetCDF.
    *
    * If the variable is not found then an Array with the element 0.0
    * is returned with shape (1,1). Note that all dimensions of size 1 are eliminated.
    * For example the array shape (21, 5, 1, 2) is reduced to (21, 5, 2) since the 3rd dimension
    * only ranges over a single value.
    *
    * @param netcdfFile The NetCDF file.
    * @param variable The variable whose data we want to extract.
    * @return Data and shape arrays.
    */
  def netCDFArrayAndShape(netcdfFile: NetcdfDataset, variable: String): (Array[Double], Array[Int]) = {
    val searchVariableArray = getNetCDFVariableArray(netcdfFile, variable)
    if (searchVariableArray == null) {
      logger.error("Variable '%s' not found. Can't create array. Returning empty array.".format(variable))
      return (Array(0.0), Array(1, 1))
    }
    val nativeArray = convertMa2Arrayto1DJavaArray(searchVariableArray)
    val shape = searchVariableArray.getShape.toList.filter(_ != 1).toArray
    (nativeArray, shape)
  }

  /**
    * Converts the native ma2.Array from the NetCDF library
    * to a one dimensional Java Array of Doubles.
    *
    * Two copies of the array are made, since NetCDF does not have any API
    * to tell what type the arrays are. Once the initial array of the loading
    * is completed, a type check is used to appropriately convert the values
    * into doubles. This involves a second copy.
    */
  def convertMa2Arrayto1DJavaArray(ma2Array: ma2.Array): Array[Double] = {
    var array: Array[Double] = Array(-123456789)
    // First copy of array
    val javaArray = ma2Array.copyTo1DJavaArray()

    try {
      // Second copy of Array
      if (!javaArray.isInstanceOf[Array[Double]]) {
        array = javaArray.asInstanceOf[Array[Float]].map(p => p.toDouble)
      } else {
        array = javaArray.asInstanceOf[Array[Double]]
      }
      for (i <- array.indices) if (array(i) == -9999.0) array(i) = 0.0
    } catch {
      case ex: Exception =>
        println("Error while converting a netcdf.ucar.ma2 to a 1D array. Most likely occurred with casting")
    }
    array
  }

  /**
    * Extracts a variable's data from a NetCDF file as an M2 array.
    *
    * @param netcdfFile the NetcdfDataSet to read from
    * @param variable the variable whose array we want to extract
    * @todo Ask why OCW hardcodes the latitude and longitude names
    */
  def getNetCDFVariableArray(netcdfFile: NetcdfDataset, variable: String): ma2.Array = {
    var searchVariable: ma2.Array = null
    try {
      if (netcdfFile == null) throw new IllegalStateException("NetCDFDataset was not loaded")
      val netcdfVal = netcdfFile.findVariable(variable)
      if (netcdfVal == null) throw new IllegalStateException("Variable '%s' was not loaded".format(variable))
      searchVariable = netcdfVal.read()
    } catch {
      case ex: Exception =>
        logger.error("Variable '%s' not found when reading source %s.".format(variable, netcdfFile))
        logger.info("Variables available: " + netcdfFile.getVariables)
        logger.error(ex.getMessage)
    }
    searchVariable
  }

  /**
    * Loads a NetCDF Dataset from a URL.
    */
  def loadNetCDFDataSet(url: String): NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    try {
      NetcdfDataset.openDataset(url)
    } catch {
      case e: java.io.IOException =>
        logger.error("Couldn't open dataset %s".format(url))
        null
      case ex: Exception =>
        logger.error("Something went wrong while reading %s".format(url))
        null
    }
  }

//  /**
//    * Loads a NetCDF Dataset from HDFS.
//    *
//    * @param dfsUri HDFS URI(eg. hdfs://master:9000/)
//    * @param location File path on HDFS
//    * @param bufferSize The size of the buffer to be used
//    */
//  def loadDFSNetCDFDataSet(dfsUri: String, location: String, bufferSize: Int): NetcdfDataset = {
//    import org.dia.HDFSRandomAccessFile
//    NetcdfDataset.setUseNaNs(false)
//    try {
//      val raf = new HDFSRandomAccessFile(dfsUri, location, bufferSize)
//      new NetcdfDataset(NetcdfFile.open(raf, location, null, null))
//    } catch {
//      case e: java.io.IOException =>
//        logger.error("Couldn't open dataset %s%s".format(dfsUri, location))
//        null
//      case ex: Exception =>
//        logger.error("Something went wrong while reading %s%s".format(dfsUri, location))
//        null
//    }
//  }

  /**
    * Gets the size of a dimension of a NetCDF file.
    */
  def getDimensionSize(netcdfFile: NetcdfDataset, rowDim: String): Int = {
    var dimSize = -1
    val it = netcdfFile.getDimensions.iterator()
    while (it.hasNext) {
      val d = it.next()
      if (d.getShortName.equals(rowDim))
        dimSize = d.getLength
    }
    if (dimSize < 0)
      throw new IllegalStateException("Dimension does not exist!!!")
    dimSize
  }

  def printDimensionSizes(netcdfFile: NetcdfDataset): Unit = {
    val it = netcdfFile.getDimensions.iterator()
    while (it.hasNext) {
      val d = it.next()
      println( "Dim " + d.getShortName + ": size = " + d.getLength )
    }
  }

}