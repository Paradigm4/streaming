library(poLCA)
library(parallel)
library(base64enc)
library(scidbstrm)

#
# See the interactive companion to this example that loads the example 'gss82'
# data into SciDB in examples/polca_example_interactive.R.
#
# Obtain the location of this example script:
# example=`R --slave -e "cat(system.file('examples/polca_example.R', package='scidbstrm'))"`
#
# Example with one double-valued input attribute and one double-valued output attribute:
# iquery -aq "stream(gss82, 'Rscript $example', 'format=df', 'types=double,string')"

fn <- function(x)
{
  # 1. Advance to the specified L'Ecuyer RNG stream
  RNGkind("L'Ecuyer-CMRG")
  set.seed(1)
  for(j in seq(from=1, to=x$seed[1]))
  {
    .Random.seed <<- nextRNGStream(.Random.seed)
  }

  # 2. Convert columns to factors, omit seed
  y = data.frame(lapply(x[,-5], factor))

  # 3. Compute poLCA
  f <- cbind(PURPOSE,ACCURACY,UNDERSTA,COOPERAT)~1
  p <- poLCA(f, y, nclass=4, maxiter=10000, tol=1e-7, verbose=FALSE)

  best <- p$llik

  # 4. Serialize result and return in 2-column data frame with
  # column 1: llik  (double precision value) best log likelihood
  # column 2: base64encoded serialized poLCA model
  model <- base64encode(serialize(p, NULL))
  data.frame(llik=best, model=model, stringsAsFactors=FALSE)
}

map(fn)
