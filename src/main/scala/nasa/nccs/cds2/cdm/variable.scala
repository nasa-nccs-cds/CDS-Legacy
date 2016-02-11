package nasa.nccs.cds2.cdm

import nasa.nccs.cdapi.kernels.DataFragment
import nasa.nccs.cds2.cdm
import nasa.nccs.cds2.loaders.Collection
import java.util.Date
import nasa.nccs.cds2.tensors.Nd4jTensor
import nasa.nccs.cds2.utilities.cdsutils
import nasa.nccs.esgf.utilities.numbers.GenericNumber
import org.nd4j.linalg.indexing.{NDArrayIndex, INDArrayIndex}
import ucar.nc2.time.{CalendarDate, CalendarDateRange}
import nasa.nccs.esgf.process.DomainAxis
import nasa.nccs.cdapi.tensors.AbstractTensor
import org.nd4j.linalg.api.ndarray.INDArray
import ucar.{ma2, nc2}
import ucar.nc2.Variable
import ucar.nc2.dataset.{CoordinateAxis, CoordinateAxis1D, CoordinateSystem, CoordinateAxis1DTime}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import ucar.nc2.constants.AxisType

object BoundsRole extends Enumeration { val Start, End = Value }

object CDSVariable { }

class CDSVariable(val name: String, val dataset: CDSDataset, val ncVariable: nc2.Variable) {
  val logger = org.slf4j.LoggerFactory.getLogger("nasa.nccs.cds2.cdm.CDSVariable")
  val description = ncVariable.getDescription
  val dims = ncVariable.getDimensionsAll.toList
  val units = ncVariable.getUnitsString
  val shape = ncVariable.getShape.toList
  val fullname = ncVariable.getFullName
  val attributes = nc2.Attribute.makeMap(ncVariable.getAttributes).toMap
  val subsets = mutable.ListBuffer[Fragment]()

  override def toString = "\nCDSVariable(%s) { description: '%s', shape: %s, dims: %s, }\n  --> Variable Attributes: %s".format(name, description, shape.mkString("[", " ", "]"), dims.mkString("[", ",", "]"), attributes.mkString("\n\t\t", "\n\t\t", "\n"))

  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"").toLowerCase

  def getBoundedCalDate(coordAxis1DTime: CoordinateAxis1DTime, caldate: CalendarDate, role: BoundsRole.Value, strict: Boolean = true): CalendarDate = {
    val date_range: CalendarDateRange = coordAxis1DTime.getCalendarDateRange
    if (!date_range.includes(caldate)) {
      if (strict) throw new IllegalStateException("CDS2-CDSVariable: Time value %s outside of time bounds %s".format(caldate.toString, date_range.toString))
      else {
        if (role == BoundsRole.Start) {
          val startDate: CalendarDate = date_range.getStart
          logger.warn("Time value %s outside of time bounds %s, resetting value to range start date %s".format(caldate.toString, date_range.toString, startDate.toString))
          startDate
        } else {
          val endDate: CalendarDate = date_range.getEnd
          logger.warn("Time value %s outside of time bounds %s, resetting value to range end date %s".format(caldate.toString, date_range.toString, endDate.toString))
          endDate
        }
      }
    } else caldate
  }

  def getTimeCoordIndex(coordAxis: CoordinateAxis, tval: String, role: BoundsRole.Value, strict: Boolean = true): Int = {
    val indexVal: Int = coordAxis match {
      case coordAxis1DTime: CoordinateAxis1DTime =>
        val caldate: CalendarDate = cdsutils.dateTimeParser.parse(tval)
        val caldate_bounded: CalendarDate = getBoundedCalDate(coordAxis1DTime, caldate, role, strict)
        coordAxis1DTime.findTimeIndexFromCalendarDate(caldate_bounded)
      case _ => throw new IllegalStateException("CDS2-CDSVariable: Can't process time axis type: " + coordAxis.getClass.getName)
    }
    indexVal
  }

  def getTimeIndexBounds(coordAxis: CoordinateAxis, startval: String, endval: String, strict: Boolean = true): ma2.Range = {
    val startIndex = getTimeCoordIndex(coordAxis, startval, BoundsRole.Start, strict)
    val endIndex = getTimeCoordIndex(coordAxis, endval, BoundsRole.End, strict)
    new ma2.Range(startIndex, endIndex)
  }

  def getGridCoordIndex(coordAxis: CoordinateAxis, cval: Double, role: BoundsRole.Value, strict: Boolean = true): Int = {
    coordAxis match {
      case coordAxis1D: CoordinateAxis1D =>
        coordAxis1D.findCoordElement(cval) match {
          case -1 =>
            if (role == BoundsRole.Start) {
              val grid_start = coordAxis1D.getCoordValue(0)
              logger.warn("Axis %s: ROI Start value %f outside of grid area, resetting to grid start: %f".format(coordAxis.getShortName, cval, grid_start))
              0
            } else {
              val end_index = coordAxis1D.getSize.toInt - 1
              val grid_end = coordAxis1D.getCoordValue(end_index)
              logger.warn("Axis %s: ROI Start value %s outside of grid area, resetting to grid start: %f".format(coordAxis.getShortName, cval, grid_end))
              end_index
            }
          case ival => ival
        }
      case _ => throw new IllegalStateException("CDS2-CDSVariable: Can't process xyz coord axis type: " + coordAxis.getClass.getName)
    }
  }

  def getGridIndexBounds(coordAxis: CoordinateAxis, startval: Double, endval: Double, strict: Boolean = true): ma2.Range = {
    val startIndex = getGridCoordIndex(coordAxis, startval, BoundsRole.Start, strict)
    val endIndex = getGridCoordIndex(coordAxis, endval, BoundsRole.End, strict)
    new ma2.Range(startIndex, endIndex)
  }

  def getIndexBounds(coordAxis: CoordinateAxis, startval: GenericNumber, endval: GenericNumber, strict: Boolean = true): ma2.Range = {
    val indexRange = if (coordAxis.getAxisType == nc2.constants.AxisType.Time) getTimeIndexBounds(coordAxis, startval.toString, endval.toString ) else getGridIndexBounds(coordAxis, startval, endval)
    assert(indexRange.last >= indexRange.first, "CDS2-CDSVariable: Coordinate bounds appear to be inverted: start = %s, end = %s".format(startval.toString, endval.toString))
    indexRange
  }

  def getSubSection( roi: List[DomainAxis] ): ma2.Section = {
    val shape = ncVariable.getRanges.to[mutable.ArrayBuffer]
    for (axis <- roi) {
      dataset.getCoordinateAxis(axis.axistype) match {
        case Some(coordAxis) =>
          ncVariable.findDimensionIndex(coordAxis.getShortName) match {
            case -1 => throw new IllegalStateException("CDS2-CDSVariable: Can't find axis %s in variable %s".format(coordAxis.getShortName, ncVariable.getNameAndDimensions))
            case dimension_index =>
              axis.system match {
                case "indices" =>
                  shape.update(dimension_index, new ma2.Range(axis.start.toInt, axis.end.toInt, 1))
                case "values" =>
                  val boundedRange = getIndexBounds(coordAxis, axis.start, axis.end)
                  shape.update(dimension_index, boundedRange)
                case _ => throw new IllegalStateException("CDS2-CDSVariable: Illegal system value in axis bounds: " + axis.system)
              }
          }
        case None => logger.warn("Ignoring bounds on %s axis in variable %s".format(axis.name, ncVariable.getNameAndDimensions))
      }
    }
    new ma2.Section( shape )
  }

  def getNDArray( array: ucar.ma2.Array ): INDArray = {
    import org.nd4j.linalg.factory.Nd4j
    val t0 = System.nanoTime
    val result = array.getElementType.toString match {
      case "float" =>
        val java_array = array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Float]]
        Nd4j.create( java_array, array.getShape )
      case "int" =>
        val java_array = array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Int]]
        Nd4j.create( java_array, array.getShape )
      case "double" =>
        val java_array = array.get1DJavaArray( array.getElementType ).asInstanceOf[Array[Double]]
        Nd4j.create( java_array, array.getShape )
    }
    val t1 = System.nanoTime
    logger.info( "Converted java array to INDArray, shape = %s, time = %.2f ms".format( array.getShape.toList.toString, (t1-t0)/1e6 ) )
    result
  }

  def loadRoi(roi: List[DomainAxis]): DataFragment = {
    val roiSection: ma2.Section = getSubSection(roi)
    findSubset(roiSection) match {
      case None =>
        val array = ncVariable.read(roiSection)
        val ndArray: INDArray = getNDArray(array)
        addSubset( roiSection, new Nd4jTensor(ndArray) )
      case Some(subset) =>
        subset
    }
  }

  def addSubset( roiSection: ma2.Section, array: AbstractTensor ): DataFragment = {
    val subset = new Fragment( array, roiSection )
    subsets += subset
    subset
  }

  def findSubset( requestedSection: ma2.Section, copy: Boolean=false ): Option[Fragment] = {
    val validSubsets = subsets.filter( _.contains(requestedSection) )
    validSubsets.size match {
      case 0 => None;
      case _ => Some( validSubsets.minBy( _.size ).cutNewSubset(requestedSection, copy ) )
    }
  }
}

object Fragment {
  def sectionToIndices( section: ma2.Section ): List[(Int,Int)] = section.getRanges.map( range => ( range.first, range.last ) ).toList
}

class Fragment( array: AbstractTensor, roiSection: ma2.Section, metaDataVar: (String, String)*  ) extends DataFragment( array, metaDataVar:_* ) {
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  override def toString = { "Fragment: shape = %s, section = %s".format( array.shape.toString, roiSection.toString ) }

  def cutNewSubset( newSection: ma2.Section, copy: Boolean ): Fragment = {
    if (roiSection.equals( newSection )) this
    else {
      val relativeSection = newSection.shiftOrigin( roiSection )
      val newDataArray = array( Fragment.sectionToIndices(relativeSection):_* )
      new Fragment( if(copy) array.dup() else newDataArray, newSection )
    }
  }
  def size: Long = roiSection.computeSize
  def contains( requestedSection: ma2.Section ): Boolean = roiSection.contains( requestedSection )
}

object sectionTest extends App {
  val s0 = new ma2.Section( Array(10,10,0), Array(100,100,10) )
  val s1 = new ma2.Section( Array(50,50,5), Array(10,10,1) )
  val s2 = s1.shiftOrigin( s0 )
  println( s2 )
}

