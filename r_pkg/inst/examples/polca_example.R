library(poLCA)
library(base64enc)
library(scidb)
scidbconnect()

# The following program will be run by R processes invoked
# on each SciDB instance.
program <- as.scidb(base64encode(serialize(expression(
{
  library(poLCA)
  library(parallel)
  library(base64enc)
  library(scidbstrm)

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
}), NULL)))




# An example from the poLCA vignette:
data(gss82)
n <- nrow(gss82)
INSTANCES <- nrow(scidbls(type='instances'))

# Replicate and upload the data once per SciDB instance, adding an integer seed
# values that vary by instance.
repl <- Reduce(rbind, Map(function(j) cbind(gss82, seed=j), 1:INSTANCES))
x <- scidbeval(repart(as.scidb(repl), chunk=n), name='gss82')


# Run the experiment using the SciDB streaming API in parallel.
query <- sprintf("stream(gss82, 'R --slave -e \"library(scidbstrm);eval(unserialize(base64enc::base64decode(getChunk()[[1]])))\"', 'format=df', 'types=double,string', 'names=llik,model', _sg(%s,0))", program@name)
result <- iquery(query, return=TRUE)

llik <- result$llik
# Convert the encoded models back into R objects
models <- lapply(result$model, function(x) unserialize(base64decode(x)))

# A table of likelihoods and models
print(cbind(llik, models))

# One model with the best llik
print(models[which(llik == max(llik))[1]])
