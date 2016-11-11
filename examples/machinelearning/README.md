This is a companion directory to the vignette:
https://github.com/Paradigm4/stream/blob/master/r_pkg/vignettes/advanced_example.Rmd

This directory contain the individual Rscripts outlined in that vignette. 

Before running the commands in the vignette, move all the scripts from this folder (except `final.R`) to the `/tmp` directory:
```
cp bootstrap.R /tmp/
cp combine.R /tmp/
cp predict.R /tmp/
```

The final results of the machine learning can be seen by stepping through `final.R`, or by running
```
R --slave -e 'source("final.R")'
```
