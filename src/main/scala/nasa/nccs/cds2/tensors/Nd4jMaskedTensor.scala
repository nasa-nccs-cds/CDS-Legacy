package nasa.nccs.cds2.tensors

import nasa.nccs.cdapi.tensors.{TensorOp, AbstractTensor}
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.cpu.NDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._
import scala.collection.immutable.IndexedSeq
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class Nd4jMaskedTensor(val tensor: INDArray = new NDArray(), val invalid: Float = Float.NaN ) extends AbstractTensor {
  override type T = Nd4jMaskedTensor
  val name: String = "nd4jm"
  val shape = tensor.shape

  override def toString = {
    val tstr = tensor.data().toString
    "Nd4jMaskedTensor[%s]: %s".format( shape.mkString(","), tstr )
  }

  def subset( index: Int, dimensions: Int*  ): Nd4jMaskedTensor = {
    new Nd4jMaskedTensor( tensor.tensorAlongDimension(index, dimensions:_*), invalid )
  }

  def exec( op: TensorOp, dimensions: Int* ): Nd4jMaskedTensor = {
    val filtered_shape: IndexedSeq[Int] = (0 until shape.length).flatMap(x => if (dimensions.exists(_==x)) None else Some(shape(x)) )
    val slices =  Nd4j.concat( 0, (0 until filtered_shape.product ).map( iS => Nd4j.create( subset(iS,dimensions:_*).applyOp(op) ) ): _* )
    val new_shape = if (op.length == 1) filtered_shape else  filtered_shape:+op.length
    slices.setShape(new_shape:_*); slices.cleanup()
    new Nd4jMaskedTensor( slices, invalid )
  }

  def applyOp( op: TensorOp ): Array[Float] = {
    op.init
    for( iC <- 0 until tensor.length )  {
      val v = tensor.getFloat(iC)
      if( v != invalid ) op.insert(v)
    }
    op.result
  }

  def mean( dimensions: Int* ): Nd4jMaskedTensor = exec( meanOp, dimensions:_* )

  def rawmean( dimensions: Int* ): Nd4jMaskedTensor =  new Nd4jMaskedTensor( tensor.mean(dimensions:_*), invalid )

  def zeros(shape: Int*) = new Nd4jMaskedTensor(Nd4j.create(shape: _*))

  def map(f: Double => Double) = new Nd4jMaskedTensor(tensor.map(p => f(p)))

  def put(value: Float, shape: Int*) = tensor.putScalar(shape.toArray, value)

  def +(array: AbstractTensor) = new Nd4jMaskedTensor(tensor.add(array.tensor))

  def -(array: AbstractTensor) = new Nd4jMaskedTensor(tensor.sub(array.tensor))

  def \(array: AbstractTensor) = new Nd4jMaskedTensor(tensor.div(array.tensor))

  def /(array: AbstractTensor) = new Nd4jMaskedTensor(tensor.div(array.tensor))

  def *(array: AbstractTensor) = new Nd4jMaskedTensor(tensor.mul(array.tensor))

  /**
   * Masking operations
   */

  def <=(num: Float): Nd4jMaskedTensor = new Nd4jMaskedTensor(tensor.map(p => if (p < num) p else 0.0))

  def :=(num: Float) = new Nd4jMaskedTensor(tensor.map(p => if (p == num) p else 0.0))

  /**
   * Linear Algebra Operations
   */
  
  def **(array: AbstractTensor) = new Nd4jMaskedTensor(tensor.dot(array.tensor))
  
  def div(num: Float): Nd4jMaskedTensor = new Nd4jMaskedTensor(tensor.div(num))

  /**
   * SliceableArray operations
   */

  def rows = tensor.rows
  
  def cols = tensor.columns

  def apply = this

  def apply(ranges: (Int, Int)*) = {
    val rangeMap = ranges.map(p => TupleRange(p))
    val IndArray = tensor(rangeMap: _*)
    new Nd4jMaskedTensor(IndArray)
  }

  def apply(indexes: Int*) = tensor.get(indexes.toArray).toFloat

  def data: Array[Float] = tensor.data.asFloat

  /**
   * Utility Functions
   */
  
  def cumsum = tensor.sumNumber.asInstanceOf[Float]

  def dup = new Nd4jMaskedTensor(tensor.dup)

  def isZero = tensor.mul(tensor).sumNumber.asInstanceOf[Float] <= 1E-9
  
  def isZeroShortcut = tensor.sumNumber().asInstanceOf[Float] <= 1E-9

  def max = tensor.maxNumber.asInstanceOf[Float]

  def min = tensor.minNumber.asInstanceOf[Float]

  private implicit def AbstractConvert(array: AbstractTensor): Nd4jMaskedTensor = array.asInstanceOf[Nd4jMaskedTensor]
  
}

object meanOp extends TensorOp {
  var value_sum  = 0f
  var value_count = 0
  def init = { value_sum  = 0f; value_count = 0 }
  def insert( value: Float ) = { value_sum += value; value_count += 1 }
  def result = Array( value_sum / value_count )
  def length = 1
}

object tensorTest extends App {
  var shape = Array(2,2,2)
  val full_mtensor = new Nd4jMaskedTensor( Nd4j.create( Array(1f,2f,3f,4f,5f,6f,7f,8f), shape ), 0 )
  val exec_result = full_mtensor.exec( meanOp, 1 )
  println( "." )
}
