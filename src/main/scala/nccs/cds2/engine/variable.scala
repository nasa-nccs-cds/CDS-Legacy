package nccs.cds2.engine

import ucar.nc2
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object CDSVariable {

  def apply( ncVariable: nc2.Variable) = {
    val nc_attributes = ncVariable.getAttributes
    val description = ncVariable.getDescription
    val dims = ncVariable.getDimensionsAll.toList
    val units = ncVariable.getUnitsString
    val shape = ncVariable.getShape.toList
    val name = ncVariable.getFullName
    val attributes = nc2.Attribute.makeMap( nc_attributes ).toMap
    new CDSVariable( name, shape, dims, units, attributes, description )
  }
}


class CDSVariable(val name: String, val shape: List[Int], val dims: List[nc2.Dimension], val units: String, val attributes: Map[String,nc2.Attribute], val description: String = "" ) {
  override def toString = "\nCDSVariable(%s) { description: '%s', shape: %s, dims: %s, }\n  --> Variable Attributes: %s".format( name, description, shape.mkString("["," ","]"), dims.mkString("[",",","]"), attributes.mkString("\n\t\t", "\n\t\t", "\n") )
}
