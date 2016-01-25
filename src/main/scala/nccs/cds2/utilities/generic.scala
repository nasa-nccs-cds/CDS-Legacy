package nccs.cds2.utilities


object cdsutils {

  def flatlist[T]( values: Option[T]* ): List[T] = values.flatten.toList
}




