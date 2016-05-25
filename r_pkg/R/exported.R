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

#' Process SciDB streaming data frame chunks
#'
#' The SciDB streaming API works with R functions that map a data frame input value
#' to a data frame output value. The output data frame column types must match the
#' 
#' @param f a function of a single data frame input argument that returns a data frame
#' output. The output data frame column types must match the SciDB stream operator
#' 'types' argument.
#' @param convertFactor a function for conversion of R factor values into a supported type: one of double, integer, or character.
#' @param stringsAsFactors convert input strings to data frame factor values (\code{TRUE)} or not.
#' @note Factor and logical values are converted by default into integer values. Set
#' \code{convertFactor=as.character} to convert factor values to character strings instead.
#'
#' @seealso \code{\link{schema}}
#' @examples
#' \dontrun{
#' # Identity function:
#' # iquery -aq "stream(build(<val:double> [i=1:5,5,0], i), 'R --slave -e \"library(scidbstrm); run(I)\"', 'format=df', 'types=double')"
#'
#' # Return R process ids (up to 10, depending on number of SciDB instances)
#' # iquery -aq "stream(build(<val:double> [i=1:10,1,0], i), 'R --no-save --slave -e \"library(scidbstrm); f=function(x) data.frame(pid=Sys.getpid()); run(f)\"', 'format=df', 'types=int32')"
#' }
#' @export
run <- function(f, convertFactor = as.integer, stringsAsFactors=FALSE)
{
  con_in <- file("stdin", "rb") # replace with zero-copy to data frame version XXX TODO
  con_out <- pipe("cat", "wb")  # replace with direct to stdout version XXX TODO
  while( TRUE )
  {
    input_list <- unserialize(con_in)
    ncol <- length(input_list)
    if(ncol == 0) # this is the last message
    {
      writeBin(serialize(list(), NULL, xdr=FALSE), con_out)
      flush(con_out)
      q(save="no")
    }
  out <- as.list(f(data.frame(input_list, stringsAsFactors=stringsAsFactors)))
  # limit types to double, int, logical
  types <- vapply(out, class, "")
  i <- types %in% "logical"
  if(any(i)) out[i] <- lapply(out[i], as.integer)
  i <- types %in% "factor"
  if(any(i)) out[i] <- lapply(out[i], convertFactor)
  writeBin(serialize(out, NULL, xdr=FALSE), con_out)
  flush(con_out)
  }
  close(con_in)
  close(con_out)
}
