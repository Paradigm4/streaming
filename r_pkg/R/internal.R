# Internal utility function
# @param out a data frame
# @return a list suitable for writing to SciDB
asTypedList <- function(out)
{
  if(is.null(out)) return(list())
  stopifnot(is.data.frame(out))
  out <- as.list(out)
  # limit types to double, int, logical
  types <- vapply(out, class, "")
  i <- types %in% "logical"
  if(any(i)) out[i] <- lapply(out[i], as.integer)
  i <- types %in% "factor"
  if(any(i)) out[i] <- lapply(out[i], convertFactor)
  out
}
