# invoke with, for example:
# iquery -aq "stream(build(<val:double> [i=1:5,5,0], i), 'Rscript /home/blewis/streaming/src/R_script.R', 'format=df', 'types=double,int32')"

con_in = file("stdin", "rb")
con_out = pipe("cat", "wb")
while( TRUE )
{
  input_list = unserialize(con_in)
  ncol = length(input_list)
  if(ncol == 0) #this is the last message
  {
    res = list()
    writeBin(serialize(res, NULL, xdr=FALSE), con_out)
    flush(con_out)
    break
  }
  nrow = length(input_list[[1]])
  #Example modification: double the first column
  input_list[[1]] = input_list[[1]] * 2
  input_list[[2]] = as.integer(input_list[[1]] * 4)
  writeBin(serialize(input_list, NULL, xdr=FALSE), con_out)
  flush(con_out)
}
close(con_in)
