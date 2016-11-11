# An example R client that uses TSV to communicate with SciDB.
# It reads data into a data.frame of X columns and returns X+1 columns, with the last column equal to the first, times two.
# For exmaple:
#  iquery -aq "stream(build(<val:string> [i=1:5,5,0], i), 'Rscript /home/apoliakov/stream/src/tsv_R_client.R')"
#  {instance_id,chunk_no} status
#  {0,0} '1	2
#  2	4
#  3 	6
#  4	8
#  5	10'

con_in <-file("stdin")
open(con_in, open="r")
while( TRUE )
{
  #Read the header 
  a = read.table(con_in,nrow=1)
  if ( a[1,1]==0 ) #this is the last message from SciDB. We done.
  {
    write.table(a, stdout(), col.names=FALSE, row.names = FALSE)
    break;
  }
  #Read message
  b = read.table(con_in,nrow=a[1,1], sep="\t")
  #Compute new last column
  b$new = b$V1 * 2
  #Write response
  write.table(a, stdout(), col.names=FALSE, row.names = FALSE)
  write.table(b, stdout(), col.names=FALSE, row.names = FALSE, sep="\t", quote = FALSE)
  #Flushing is key: might freeze otherwize
  flush(stdout())
}
close(con_in)
