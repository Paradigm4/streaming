# invoke with, for example:
# iquery -aq "stream(build(<val:double> [i=1:5,5,0], i), 'Rscript /home/blewis/stream/src/R_script.R', 'format=df', 'types=double,int32')"

con_in = file("stdin", "rb")
con_out = pipe("cat", "wb")
total = NA
while( TRUE )
{
  input_list = unserialize(con_in)
  ncol = length(input_list)
  if(ncol == 0) #this is the last message
  {
    writeBin(serialize(list(), NULL, xdr=FALSE), con_out)
    flush(con_out)
    break
  }
  strings = input_list[[1]]
  strings = sprintf("Hello, %s!", strings)
  strings = c(strings, "")
  strings = c(strings, "Thx!")
  writeBin(serialize(list(s=strings), NULL, xdr=FALSE), con_out)
  flush(con_out)
}
close(con_in)
