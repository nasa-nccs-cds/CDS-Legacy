package nasa.nccs.cds2.engine

import ucar.nc2
import nc2.dataset.{ CoordinateAxis1D, CoordinateSystem }
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object CDSVariable {

  def apply( ncFile: nc2.NetcdfFile, ncVariable: nc2.Variable) = {
    val description = ncVariable.getDescription
    val dims = ncVariable.getDimensionsAll.toList
    val units = ncVariable.getUnitsString
    val shape = ncVariable.getShape.toList
    val name = ncVariable.getFullName
    val attributes = nc2.Attribute.makeMap( ncVariable.getAttributes ).toMap
    val gblAttributes = nc2.Attribute.makeMap( ncFile.getGlobalAttributes ).toMap
    val dsId = ncFile.getId
    val dsTitle = ncFile.getTitle()
    val dimensions = ncVariable.getDimensions().toList
    val axes = for( dimension <- dimensions) yield ncFile.findVariable( dimension.getGroup, dimension.getShortName )
    val coordAxes: List[CoordinateAxis1D] = for ( axis <- axes ) yield axis match { case coordinate_axis: CoordinateAxis1D => coordinate_axis; case x => throw new Exception( "Avis Type currently unsupported:" + x.getClass.getName ) }
    val sampleCoordAxes = coordAxes.head
    val sampleCoordSystems = sampleCoordAxes.getCoordinateSystems
    val coordSystems = mutable.HashSet[CoordinateSystem]()
    for( axis <- coordAxes; coordSystem <-axis.getCoordinateSystems ) coordSystems += coordSystem
    assert( coordSystems.size <= 1, "Multiple Coordinate Systems for a single variable not supported.")
    val coordSystemOpt: Option[CoordinateSystem] = if( coordSystems.isEmpty ) None else Some( coordSystems.head )
    new CDSVariable( name, shape, dims, units, attributes, description )
  }
}


class CDSVariable(val name: String, val shape: List[Int], val dims: List[nc2.Dimension], val units: String, val attributes: Map[String,nc2.Attribute], val description: String = "" ) {
  override def toString = "\nCDSVariable(%s) { description: '%s', shape: %s, dims: %s, }\n  --> Variable Attributes: %s".format( name, description, shape.mkString("["," ","]"), dims.mkString("[",",","]"), attributes.mkString("\n\t\t", "\n\t\t", "\n") )
}
