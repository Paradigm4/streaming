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

#' Map an R function across SciDB streaming data frame chunks.
#'
#' The SciDB streaming API works with R functions that take a data frame input value
#' and produce a data frame output value. The output data frame column types must match the
#' types declared in the SciDB stream operator.
#' 
#' @param f a function of a single data frame input argument that returns a data frame
#' output. The output data frame column types must match the SciDB stream operator
#' 'types' argument.
#' @param convertFactor a function for conversion of R factor values into one of double, integer, or character for return to SciDB.
#' @param final optional function applied to last output value before returning. If supplied, \code{final} must be a function of a
#' single data frame that returns a data frame compatible with the expected types (just like \code{f}).
#' @note Factor and logical values are converted by default into integer values. Set
#' \code{convertFactor=as.character} to convert factor values to character strings instead.
#' 
#' Nothing is returned to SciDB when then function \code{f} returns \code{NULL}. Use this in combination
#' with the \code{final} function to perform aggregation across chunks (see the examples).
#' @seealso \code{\link{schema}}
#' @examples
#' \dontrun{
#' # Identity function:
#' # iquery -aq "stream(build(<val:double> [i=1:5,5,0], i), 'R --slave -e \"library(scidbstrm); map(I)\"', 'format=df', 'types=double')"
#'
#' # Return R process ids (up to 10, depending on number of SciDB instances)
#' # iquery -aq "stream(build(<val:double> [i=1:10,1,0], i), 'R --no-save --slave -e \"library(scidbstrm); f=function(x) data.frame(pid=Sys.getpid()); map(f)\"', 'format=df', 'types=int32')"
#' }
#' @export
map <- function(f, convertFactor=as.integer, final)
{
  con_in <- file("stdin", "rb")
  con_out <- pipe("cat", "wb")
  output <- NULL
  tryCatch( # fast exit on error
    while(TRUE)
    {
      input <- data.frame(unserialize(con_in), stringsAsFactors=FALSE)
      if(nrow(input) == 0) # this is the last message
      {
        if(!missing(final))
          writeBin(serialize(asTypedList(final(output)), NULL, xdr=FALSE), con_out)
        else
          writeBin(serialize(list(), NULL, xdr=FALSE), con_out)
        q(save="no")
      }
    output <- f(input)
    writeBin(serialize(asTypedList(output), NULL, xdr=FALSE), con_out)
    flush(con_out)
    }, error=function(e) {cat(as.character(e), "\n", file=stderr()); q()})
  close(con_in)
  close(con_out)
}
