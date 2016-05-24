con_in <-file("stdin")
con_out <- pipe("cat", "wb")

open(con_in, open="rb")
while( TRUE )
{
  input_list = unserialize(con_in)
  ncol = length(input_list)
  if(ncol == 0) #this is the last message
  {
    res = list()
    writeBin(serialize(res, NULL, xdr=FALSE), con_out)
    flush(con_out)
    break;
  }
  nrow = length(input_list[[1]])
  input_list[[1]] = input_list[[1]]*2
  input_list[[2]] = as.integer(input_list[[1]]*4)
  writeBin(serialize(input_list, NULL, xdr=FALSE), con_out)
  flush(con_out)
}
close(con_in)
