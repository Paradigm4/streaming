library(scidb)
scidbconnect()
p = scidb("predictions")[]
p = p[,4:5]
names(p) = c("observed", "predicted")
print(table(p))