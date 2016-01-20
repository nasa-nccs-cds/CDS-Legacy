package nccs.utilities.numbers

class IllegalNumberException( value: Any ) extends RuntimeException("Error, " + value.toString + " is not a valid Number")

object GenericNumber {

  def fromString(sx: String): GenericNumber = {
    try {
      new IntNumber(sx.toInt)
    } catch {
      case err: NumberFormatException => try {
        new FloatNumber(sx.toFloat)
      } catch {
        case err: NumberFormatException => throw new IllegalNumberException(sx)
      }
    }
  }
  def apply( anum: Any = None ): GenericNumber = {
    anum match {
      case ix: Int =>     new IntNumber(ix)
      case fx: Float =>   new FloatNumber(fx)
      case dx: Double =>  new DoubleNumber(dx)
      case sx: Short =>   new ShortNumber(sx)
      case None =>        new UndefinedNumber()
      case sx: String =>  fromString( sx )
      case x =>           throw new IllegalNumberException(x)
    }
  }
}

abstract class GenericNumber {
  type NumericType
  def value(): NumericType
  override def toString() = { value().toString }
}

class IntNumber( val numvalue: Int ) extends GenericNumber {
  type NumericType = Int
  override def value(): NumericType = { numvalue }
}

class FloatNumber( val numvalue: Float ) extends GenericNumber {
  type NumericType = Float
  override def value(): NumericType = { numvalue }
}

class DoubleNumber( val numvalue: Double ) extends GenericNumber {
  type NumericType = Double
  override def value(): NumericType = { numvalue }
}

class ShortNumber( val numvalue: Short ) extends GenericNumber {
  type NumericType = Short
  override def value(): NumericType = { numvalue }
}

class UndefinedNumber extends GenericNumber {
  type NumericType = Option[Any]
  override def value(): NumericType = { None }
}

object testNumbers extends App {
  def testIntMethod( ival: Int ): Unit = { println( s" Got Int: $ival" ) }
  val x = GenericNumber("40")
  println( "CLASS = " + x.getClass.getName + ", VALUE = " + x.toString )
}




