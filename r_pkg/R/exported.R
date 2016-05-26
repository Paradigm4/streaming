#' Return a SciDB streaming-compatible output schema for a function
#'
#' The SciDB streaming API works with R functions that map a data frame input value
#' to a data frame output value. The `schema` utility function returns the output
#' schema for a given function that can be used in an associated SciDB
#' stream query argument.
#' @param f an R function of a single data frame argument that returns a data frame output value.
#' @param input  example data frame input argument that would be passed in practice to the function `f` by SciDB.
#' @return a character value describing the output data frame schema required by the SciDB stream operator 'types' argument.
#' @examples
#' # Identity function acting on the iris data frame
#' schema(I, iris)
#' @export
schema <- function(f, input)
{
  out <- vapply(f(input), class, "")
  names(out) <- c()
  out <- gsub("numeric", "double", out)
  out <- gsub("factor", "int32", out)
  out <- gsub("logical", "int32", out)
  out <- gsub("integer", "int32", out)
  out <- gsub("character", "string", out)
  i <- !(out %in% c("double", "string", "int32"))
  if(any(i))
  {
    stop("unspported output type(s) ", paste(out[i], collapse=" "))
  }
  paste(out, collapse=",")
}

#' Map an R function across SciDB streaming data frame chunks
#'
#' The SciDB streaming API works with R functions that take a data frame input value
#' and produce a data frame output value. The output data frame column types must match the
#' types declared in the SciDB stream operator.
#' 
#' @param f a function of a single data frame input argument that returns a data frame
#' output. The output data frame column types must match the SciDB stream operator
#' 'types' argument.
#' @param convertFactor a function for conversion of R factor values into a supported type: one of double, integer, or character.
#' @param stringsAsFactors convert input strings to data frame factor values (\code{TRUE)} or not.
#' @note Factor and logical values are converted by default into integer values. Set
#' \code{convertFactor=as.character} to convert factor values to character strings instead.
#'
#' @seealso \code{\link{schema}} \code{\link{map}}
#' @examples
#' \dontrun{
#' # Identity function:
#' # iquery -aq "stream(build(<val:double> [i=1:5,5,0], i), 'R --slave -e \"library(scidbstrm); map(I)\"', 'format=df', 'types=double')"
#'
#' # Return R process ids (up to 10, depending on number of SciDB instances)
#' # iquery -aq "stream(build(<val:double> [i=1:10,1,0], i), 'R --no-save --slave -e \"library(scidbstrm); f=function(x) data.frame(pid=Sys.getpid()); map(f)\"', 'format=df', 'types=int32')"
#' }
#' @export
map <- function(f, convertFactor=as.integer, stringsAsFactors=FALSE)
{
  con_in <- file("stdin", "rb") # replace with zero-copy to data frame version XXX TODO
  con_out <- pipe("cat", "wb")  # replace with direct to stdout version XXX TODO
  tryCatch( # fast exit on error
  while( TRUE )
  {
    input_list <- unserialize(con_in)
    ncol <- length(input_list)
    if(ncol == 0) # this is the last message
    {
      writeBin(serialize(list(), NULL, xdr=FALSE), con_out)
      q(save="no")
    }
  out <- asTypedList(f(data.frame(input_list, stringsAsFactors=stringsAsFactors)))
  writeBin(serialize(out, NULL, xdr=FALSE), con_out)
  flush(con_out)
  }, error=function(e) {cat(as.character(e), "\n", file=stderr()); q()})
  close(con_in)
  close(con_out)
}


#' Successively combine an R function across SciDB streaming data frame chunks
#'
#' This function is similar to the \code{Reduce} function, but applied to data frame chunks supplied
#' by SciDB. The SciDB streaming API works with R functions that take a data frame input value
#' and produce a data frame output value. The output data frame column types must match the
#' types declared in the SciDB stream operator.
#' 
#' @param f a binary function that takes two data frames as input argumenst and
#' produces a single output data frame.  The output data frame column types must
#' match the SciDB stream operator 'types' argument.
#' @param accumulate a logical indicating whether the successive reduce
#'        combinations should be accumulated.  By default, only the
#'        final combination is used.
#' @param init optional initial data frame value to kick off the aggregation, must have the same form as the output of f.
#' @param final optional function applied to final result before returning. If supplied, final must be a function of a
#' single data frame that returns a data frame compatible with the expected types.
#' @param convertFactor a function for conversion of R factor values into a supported type: one of double, integer, or character.
#' @param stringsAsFactors convert input strings to data frame factor values (\code{TRUE)} or not.
#' @note Factor and logical values are converted by default into integer values. Set
#' \code{convertFactor=as.character} to convert factor values to character strings instead.
#' The \code{final} function does not apply to cumulative results when \code{accumulate=TRUE}, but only to the last returned
#' result.
#'
#' Beware that data frame chunks are aggregated on a per-SciDB instance basis, and not
#' globally across all the input data.
#'
#' @seealso \code{\link{schema}} \code{\link{map}}
#' @examples
#' \dontrun{
#' }
#' @export
reduce <- function(f, init, accumulate=FALSE, final=I, convertFactor=as.integer, stringsAsFactors=FALSE)
{
  state <- NULL
  if(!missing(init)) state <- init
  con_in <- file("stdin", "rb") # replace with zero-copy to data frame version XXX TODO
  con_out <- pipe("cat", "wb")  # replace with direct to stdout version XXX TODO
  tryCatch( # fast exit on error
  while(TRUE)
  {
    input_list <- data.frame(unserialize(con_in), stringsAsFactors=stringsAsFactors)
    ncol <- length(input_list)
    if(ncol == 0) # this is the last message, return aggregated result
    {
      writeBin(serialize(asTypedList(final(state)), NULL, xdr=FALSE), con_out)
      flush(con_out)
      q(save="no")
    }
    if(is.null(state))
    {
      state <- input_list
    } else state <- f(state, input_list)
    if(accumulate)
    {
      writeBin(serialize(asTypedList(state), NULL, xdr=FALSE), con_out)
      flush(con_out)
      next
    }
    writeBin(serialize(list(), NULL, xdr=FALSE), con_out)
    flush(con_out)
  }, error=function(e) {cat(as.character(e), file=stderr()); q()})
  close(con_in)
  close(con_out)
}
