# Compiler settings for SciDB version >= 15.7
ifneq ("$(wildcard /usr/bin/g++-4.9)","")
  CC := "/usr/bin/gcc-4.9"
  CXX := "/usr/bin/g++-4.9"
else
  ifneq ("$(wildcard /opt/rh/devtoolset-3/root/usr/bin/gcc)","")
   CC := "/opt/rh/devtoolset-3/root/usr/bin/gcc"
   CXX := "/opt/rh/devtoolset-3/root/usr/bin/g++"
  endif
endif

all: stream_test_client

stream_test_client: client.cpp
	$(CXX) client.cpp -ggdb -o stream_test_client
		
clean:
	rm -f stream_test_client