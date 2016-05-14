all:
	$(MAKE) -C src
	@cp src/*.so .

clean:
	$(MAKE) -C src clean
	rm -f *.so