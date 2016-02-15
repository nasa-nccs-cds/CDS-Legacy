package nasa.nccs.cds2.kernels

import nasa.nccs.cdapi.kernels.DataFragment
import nasa.nccs.cds2.tensors.Nd4jTensor
import org.nd4j.linalg.api.ndarray.INDArray

trait KernelTools {

  def getNdArray( datafrags: List[DataFragment], index: Int = 0 ): ( INDArray, Float ) = {
    try {
        datafrags(index).data match {
        case indtensor: Nd4jTensor => ( indtensor.tensor, indtensor.invalid );
        case x => throw new IllegalStateException("Found unsupported array type: %s for kernel input %d".format( x.getClass.getName, index ) )
      }
    } catch {
      case err: NoSuchElementException => throw new IllegalStateException("Missing Input %d to kernel".format(index) )
    }
  }




}
