library(poLCA)
library(scidb)

# An example from the poLCA vignette:
data(gss82)
n <- nrow(gss82)

# Total number of replications to perform. The experiment is reproducible for the
# same number of replications, independently from the number of SciDB instances.
REPLICATIONS <- 8

# Replicate and upload the data once per SciDB instance, adding an integer seed values
scidbconnect()
repl <- Reduce(rbind, Map(function(j) cbind(gss82, seed=j), 1:REPLICATIONS))
x <- scidbeval(repart(as.scidb(repl), chunk=n), name='gss82')


# Run the experiment using the SciDB streaming API and the companion example program
# to this one, examples/polca_example.R
example <- system.file('examples/ex1.R', package='scidbstrm')
query <- sprintf("stream(gss82, 'Rscript %s','format=df', 'types=double,string', 'names=llik,model')", example)
result <- iquery(query, return=TRUE)

llik <- result$llik
# Convert the encoded models back into R objects
models <- lapply(result$model, function(x) unserialize(base64decode(x)))

# A table of likelihoods and models
print(cbind(llik, models))

# One model with the best llik
print(models[which(llik == max(llik))[1]])
