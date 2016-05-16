all:
	$(MAKE) -C src
	@cp src/*.so client .

clean:
	$(MAKE) -C src clean
	rm -f *.so