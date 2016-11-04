#!/usr/bin/python

import sys
end_of_interaction = 0

while (end_of_interaction != 1):
  header = sys.stdin.readline().rstrip()
  if(header != "0"):
    #We receive a message from the SciDB instance:
    num_lines = int(header)  #how many lines did we get?
    
    #Collect all lines into a list:
    input_lines = []
    for i in range(0, num_lines):
      line = sys.stdin.readline().rstrip()
      input_lines.append(line)
    
    #Print a response: 
    print(num_lines+1)
    for i in range(0, num_lines):
       print("I got\t" + input_lines[i])
    print("THX!")
    sys.stdout.flush()
    #This will appear in the scidb-sterr.log file:
    sys.stderr.write("I got a chunk with "+ str(num_lines) + " lines of text!\n")
  else:
    #If we receive "0", it means the SciDB instance has no more
    #Data to give us. Here we have the option of also responding with "0"
    #Or sending some other message (i.e. a global sum):
    end_of_interaction = 1
    print("1")
    print("KTHXBYE")
    sys.stdout.flush()
    sys.stderr.write("I got the end-of-data message. Exiting.\n")
