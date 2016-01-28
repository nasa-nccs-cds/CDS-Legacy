package nasa.nccs.cds2.utilities


object cdsutils {

  def flatlist[T]( values: Option[T]* ): List[T] = values.flatten.toList
  def findNonNull[T]( values: T* ): Option[T] = values.toList.find( _ != null )
}




