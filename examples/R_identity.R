# invoke with, for example:
# iquery -aq "stream(build(<val:double> [i=1:5,5,0], i), 'Rscript /home/blewis/stream/src/R_script.R', 'format=df', 'types=double,int32')"

con_in = file("stdin", "rb")
con_out = pipe("cat", "wb")
while( TRUE )
{
  input_list = unserialize(con_in)
  ncol = length(input_list)
  if(ncol == 0) #this is the last message
  {
    res = list()
    writeBin(serialize(res, NULL, xdr=FALSE, version=2), con_out)
    flush(con_out)
    break
  }
  writeBin(serialize(input_list, NULL, xdr=FALSE, version=2), con_out)
  flush(con_out)
}
close(con_in)
