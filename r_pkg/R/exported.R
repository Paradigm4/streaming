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
#' @param final optional function applied to last output value before returning. If supplied, \code{final} must be a function of a
#' single data frame that returns a data frame compatible with the expected types (just like \code{f}).
#' @param convertFactor a function for conversion of R factor values into one of double, integer, or character for return to SciDB.
#' @note Factor and logical values are converted by default into integer values. Set
#' \code{convertFactor=as.character} to convert factor values to character strings instead.
#' 
#' Nothing is returned to SciDB when then function \code{f} returns \code{NULL}. Use this in combination
#' with the \code{final} function to perform aggregation across chunks (see the examples).
#' @seealso \code{\link{schema}}
#' @examples
#' # (Run all the examples from a command line)
#' # Identity function:
#' # iquery -aq "stream(build(<val:double> [i=1:5,5,0], i), 'R --slave -e \"library(scidbstrm); map(I)\"', 'format=df', 'types=double')"
#'
#' # Three supported types so far are double, string, int32.
#' # NAs work for all of them and are translated to SciDB NULL.
#' # iquery -aq "stream(apply(build(<x:double> [i=1:5,5,0], i), y, 'cazart', z, int32(x)), 'R --slave -e \"library(scidbstrm); map(function(x) {x[3,] = NA;x})\"', 'format=df', 'types=double,string,int32')"
#'
#' # See more examples in the following directory:
#' system.file('examples', package='scidbstrm')
#' @export
map <- function(f, final, convertFactor=as.integer)
{
  # Check for already opened connections, closed at end of this function
  if(!exists("con_in", envir=.scidbstream.env)) .scidbstream.env$con_in <- file("stdin", "rb")
  if(!exists("con_out", envir=.scidbstream.env)) .scidbstream.env$con_out <- pipe("cat", "wb")
  output <- NULL
  tryCatch( # fast exit on error
    while(TRUE)
    {
      input <- data.frame(unserialize(.scidbstream.env$con_in), stringsAsFactors=FALSE)
      if(nrow(input) == 0) # this is the last message
      {
        if(!missing(final))
          writeBin(serialize(asTypedList(final(output), convertFactor), NULL, xdr=FALSE), .scidbstream.env$con_out)
        else
          writeBin(serialize(list(), NULL, xdr=FALSE), .scidbstream.env$con_out)
        q(save="no")
      }
    output <- f(input)
    writeBin(serialize(asTypedList(output, convertFactor), NULL, xdr=FALSE), .scidbstream.env$con_out)
    flush(.scidbstream.env$con_out)
    }, error=function(e) {cat(as.character(e), "\n", file=stderr()); q()})
  closeStreams()
}

#' Obtain a single chunk from SciDB, returning \code{output}.
#'
#' A low-level, chunk-by-chunk interface to the SciDB streaming API.
#' Use this with the optional extra array argument in the SciDB stream
#' operator to read data before running \code{map}.
#' @param output A list with chunk output to return to SciDB.
#' @return an R list of values representing one SciDB chunk
#' @seealso \code{\link{map}} \code{\link{closeStream}}
#' @export
getChunk <- function(output = list())
{
  if(!exists("con_in", envir=.scidbstream.env)) .scidbstream.env$con_in <- file("stdin", "rb")
  if(!exists("con_out", envir=.scidbstream.env)) .scidbstream.env$con_out <- pipe("cat", "wb")
  ans <- unserialize(.scidbstream.env$con_in)
  writeBin(serialize(output, NULL, xdr=FALSE), .scidbstream.env$con_out)
  ans
}

#' Explicitly close SciDB streams
#'
#' You should almost never need to call this in practice. The \code{\link{map}}
#' function automatically closes SciDB streams when done.
#' @return \code{NULL}
#' @export
closeStreams <- function()
{
  if(exists("con_in", envir=.scidbstream.env)) close(.scidbstream.env$con_in)
  if(exists("con_out", envir=.scidbstream.env)) close(.scidbstream.env$con_out)
  invisible(NULL)
}
