library(scidbstrm)
library(jsonlite)
library(randomForest)

# Obtain the model from SciDB
model <- unserialize(base64_dec(getChunk()[[1]]))

# Predict and return two columns: observed (true) data and our prediction
f <- function(x)
{
  p <- predict(model, newdata=x)
  data.frame(observed=x[[18]],  predicted=as.character(p), stringsAsFactors=FALSE)
}

# Stream SciDB data through our model prediction function
map(f)
