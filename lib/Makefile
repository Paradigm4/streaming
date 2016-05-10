all: serial slave test

serial:
	$(CC) -Wall -c serial.c

slave:
	$(CC) -Wall -c slave.c

test: serial slave
	$(CC) -o test test.c serial.o slave.o
	./test R --slave -e "a=readBin(file('stdin', open='rb'), what=raw(), n=1000);print(a);print(serialize(list(x=as.integer(1:5), y=as.double(1:5)), NULL, xdr=FALSE));print(unserialize(a))"

tsv: serial slave
	$(CC) -o test_tsv test_tsv.c serial.o slave.o
	./test_tsv cat

clean:
	rm -rf *.o *.so test test_tsv
