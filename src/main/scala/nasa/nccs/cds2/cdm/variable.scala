package nasa.nccs.cds2.cdm

import nasa.nccs.cds2.cdm
import nasa.nccs.cds2.loaders.Collection
import java.util.Date
import ucar.nc2.time.{ CalendarDate, CalendarDateRange }
import nasa.nccs.cds2.utilities.NetCDFUtils
import nasa.nccs.esgf.process.DomainAxis
import org.nd4j.linalg.api.ndarray.INDArray
import ucar.{ma2, nc2}
import ucar.nc2.Variable
import ucar.nc2.dataset.{ CoordinateAxis1D, CoordinateSystem, CoordinateAxis1DTime }
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object CDSVariable {

}

class CDSVariable( val name: String, val dataset: CDSDataset, val ncVariable: nc2.Variable ) {
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.cdm.CDSVariable")
  val description = ncVariable.getDescription
  val dims = ncVariable.getDimensionsAll.toList
  val units = ncVariable.getUnitsString
  val shape = ncVariable.getShape.toList
  val fullname = ncVariable.getFullName
  val attributes = nc2.Attribute.makeMap( ncVariable.getAttributes ).toMap
  override def toString = "\nCDSVariable(%s) { description: '%s', shape: %s, dims: %s, }\n  --> Variable Attributes: %s".format( name, description, shape.mkString("["," ","]"), dims.mkString("[",",","]"), attributes.mkString("\n\t\t", "\n\t\t", "\n") )

  def loadRoi( roi: List[DomainAxis] ) = {
    val subsetted_ranges: List[ma2.Range] = getSubsetRanges(roi)
    val array = ncVariable.read(subsetted_ranges.asJava)
    val ndArray: INDArray = NetCDFUtils.getNDArray(array)
  }

  def getSubsetRanges( roi: List[DomainAxis] ): List[ma2.Range] = {
    val shape: mutable.ArrayBuffer[ma2.Range] = ncVariable.getRanges.to[mutable.ArrayBuffer]
    for( axis <- roi ) {
      dataset.getCoordinateAxis(axis.dimension) match {
        case Some(coordAxis) =>
          ncVariable.findDimensionIndex(coordAxis.getShortName) match {
            case -1 => throw new IllegalStateException("Can't find axis %s in variable %s".format(coordAxis.getShortName, ncVariable.getNameAndDimensions))
            case dimension_index =>
              axis.system match {
                case "indices" =>
                  shape.update(dimension_index, new ma2.Range(axis.start, axis.end, 1))
                case "values" =>
                  coordAxis.getAxisType match {
                    case nc2.constants.AxisType.Time =>
                      coordAxis match {
                        case coordAxis1DTime: CoordinateAxis1DTime =>
                          val date_range: CalendarDateRange = coordAxis1DTime.getCalendarDateRange()
                          val start_date: CalendarDate = cdsutils.parseDate(axis.start)
                          if (!date_range.includes(start_date)) {

                          }
                          val start = coordAxis1DTime.findTimeIndexFromCalendarDate(start_date)
                          val end_date: CalendarDate = cdsutils.parseDate(axis.end)
                          val start = coordAxis1DTime.findTimeIndexFromCalendarDate(end_date)
                        case _ => throw new IllegalStateException("Can't process time axis type: " + coordAxis.getClass.getName )
                      }
                    case _ =>
                      coordAxis match {
                        case coordAxis1D: CoordinateAxis1D =>
                          val start: Int = coordAxis1D.findCoordElement(axis.start) match {
                            case -1 =>
                              val grid_start = coordAxis1D.getCoordValue( 0 )
                              logger.warn( "Axis %s: ROI Start value %s outside of grid area, resetting to grid start: %f".format(coordAxis.getShortName,axis.start.toString, grid_start ))
                              0
                            case x=> x
                          }
                          val end: Int = coordAxis1D.findCoordElement(axis.end) match {
                            case -1 =>
                              val grid_end = coordAxis1D.getCoordValue( coordAxis1D.getSize-1 )
                              logger.warn( "Axis %s: ROI End value %s outside of grid area, resetting to grid end: %f".format(coordAxis.getShortName,axis.end.toString, grid_end ))
                              coordAxis1D.getNumElements-1
                            case x=> x
                          }
                          assert(end > start, "Coordinate bounds appear to be inverted: start = %s, end = %s".format(axis.start.toString, axis.end.toString ))
                          shape.update(dimension_index, new ma2.Range( start, end, 1))
                        case _ => throw new IllegalStateException("Can't process xyz coord axis type: " + coordAxis.getClass.getName )
                      }
                  }
                case _ => throw new IllegalStateException("Illegal system value in axis bounds: " + axis.system)
              }

          }
        case None => logger.warn("Ignoring bounds on %c axis in variable %s".format(axis.dimension, ncVariable.getNameAndDimensions))
      }
    }
    shape.toList
  }
}
