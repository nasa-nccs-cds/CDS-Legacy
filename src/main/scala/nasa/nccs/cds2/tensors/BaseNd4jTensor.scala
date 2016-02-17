package nasa.nccs.cds2.tensors

import nasa.nccs.cdapi.tensors.{TensorOp, AbstractTensor}
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.cpu.NDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._
import scala.language.implicitConversions

abstract class BaseNd4jTensor( val tensor: INDArray = new NDArray() ) extends AbstractTensor {
  override type T <: BaseNd4jTensor
  val name: String = "BaseNd4jTensor"
  val shape = tensor.shape

  override def toString = {
    "%s[%s]: %s".format(name, shape.mkString(","), tensor.data().toString)
  }

  def subset( index: Int, dimensions: Int*  ): T

  def execOp(op: TensorOp, dimensions: Int*): INDArray = {
    val filtered_shape: IndexedSeq[Int] = (0 until shape.length).flatMap(x => if (dimensions.exists(_ == x)) None else Some(shape(x)))
    val slices = Nd4j.concat(0, (0 until filtered_shape.product).map(iS => Nd4j.create(subset(iS, dimensions: _*).applyOp(op))): _*)
    val new_shape = if (op.length == 1) filtered_shape else filtered_shape :+ op.length
    slices.setShape(new_shape: _*);
    slices.cleanup()
    slices
  }

  def applyOp( op: TensorOp ): Array[Float] = {
    op.init
    for( iC <- 0 until tensor.length )  op.insert(tensor.getFloat(iC))
    op.result
  }
}

