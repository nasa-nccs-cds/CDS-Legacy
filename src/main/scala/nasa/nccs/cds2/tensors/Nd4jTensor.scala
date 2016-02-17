package nasa.nccs.cds2.tensors

import nasa.nccs.cdapi.tensors.{TensorOp, AbstractTensor}
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.cpu.NDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._
import scala.collection.immutable.IndexedSeq
import scala.language.implicitConversions

class Nd4jTensor( val tensor: INDArray = new NDArray() ) extends AbstractTensor {
  override type T = Nd4jTensor
  val name: String = "Nd4jTensor"
  val shape = tensor.shape

  override def toString = {
    "%s[%s]: %s".format( name, shape.mkString(","), tensor.data().toString )
  }

  def subset( index: Int, dimensions: Int*  ): Nd4jTensor = {
    new Nd4jTensor( tensor.tensorAlongDimension(index, dimensions:_*) )
  }

  def execOp( op: TensorOp, dimensions: Int* ): INDArray = {
    val filtered_shape: IndexedSeq[Int] = (0 until shape.length).flatMap(x => if (dimensions.exists(_==x)) None else Some(shape(x)) )
    val slices =  Nd4j.concat( 0, (0 until filtered_shape.product ).map( iS => Nd4j.create( subset(iS,dimensions:_*).applyOp(op) ) ): _* )
    val new_shape = if (op.length == 1) filtered_shape else  filtered_shape:+op.length
    slices.setShape(new_shape:_*); slices.cleanup()
    slices
  }

  def exec( op: TensorOp, dimensions: Int* ): Nd4jTensor = {
    val slices = execOp( op, dimensions:_* )
    new Nd4jTensor( slices )
  }

  def applyOp( op: TensorOp ): Array[Float] = {
    op.init
    for( iC <- 0 until tensor.length )  op.insert(tensor.getFloat(iC))
    op.result
  }

  def mean( dimension: Int* ) =  new Nd4jTensor( tensor.mean( dimension: _* ) )

  def zeros(shape: Int*) = new Nd4jTensor(Nd4j.create(shape: _*))

  def map(f: Double => Double) = new Nd4jTensor(tensor.map(p => f(p)))

  def put(value: Float, shape: Int*) = tensor.putScalar(shape.toArray, value)

  def +(array: AbstractTensor) = new Nd4jTensor(tensor.add(array.tensor))

  def -(array: AbstractTensor) = new Nd4jTensor(tensor.sub(array.tensor))

  def \(array: AbstractTensor) = new Nd4jTensor(tensor.div(array.tensor))

  def /(array: AbstractTensor) = new Nd4jTensor(tensor.div(array.tensor))

  def *(array: AbstractTensor) = new Nd4jTensor(tensor.mul(array.tensor))

  /**
   * Masking operations
   */

  def <=(num: Float): Nd4jTensor = new Nd4jTensor(tensor.map(p => if (p < num) p else 0.0))

  def :=(num: Float) = new Nd4jTensor(tensor.map(p => if (p == num) p else 0.0))

  /**
   * Linear Algebra Operations
   */
  
  def **(array: AbstractTensor) = new Nd4jTensor(tensor.dot(array.tensor))
  
  def div(num: Float): Nd4jTensor = new Nd4jTensor(tensor.div(num))

  /**
   * SliceableArray operations
   */

  def rows = tensor.rows
  
  def cols = tensor.columns

  def apply = this

  def apply(ranges: (Int, Int)*) = {
    val rangeMap = ranges.map(p => TupleRange(p))
    val IndArray = tensor(rangeMap: _*)
    new Nd4jTensor(IndArray)
  }

  def apply(indexes: Int*) = tensor.get(indexes.toArray).toFloat

  def data: Array[Float] = tensor.data.asFloat

  /**
   * Utility Functions
   */
  
  def cumsum = tensor.sumNumber.asInstanceOf[Float]

  def dup = new Nd4jTensor(tensor.dup)

  def isZero = tensor.mul(tensor).sumNumber.asInstanceOf[Float] <= 1E-9
  
  def isZeroShortcut = tensor.sumNumber().asInstanceOf[Float] <= 1E-9

  def max = tensor.maxNumber.asInstanceOf[Float]

  def min = tensor.minNumber.asInstanceOf[Float]

  private implicit def AbstractConvert(array: AbstractTensor): Nd4jTensor = array.asInstanceOf[Nd4jTensor]
  
}
