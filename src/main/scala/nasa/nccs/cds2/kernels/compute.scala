package nasa.nccs.cds2.kernels

import nasa.nccs.cdapi.kernels.DataFragment
import nasa.nccs.cdapi.tensors.Nd4jMaskedTensor

trait KernelTools {

  def getNdArray( datafrags: List[DataFragment], index: Int = 0 ): Nd4jMaskedTensor = {
    try {
        datafrags(index).data match {
        case indtensor: Nd4jMaskedTensor => indtensor
        case x => throw new IllegalStateException("Found unsupported array type: %s for kernel input %d".format( x.getClass.getName, index ) )
      }
    } catch {
      case err: NoSuchElementException => throw new IllegalStateException("Missing Input %d to kernel".format(index) )
    }
  }




}
