all:
	$(MAKE) -C src
	@cp src/*.so src/client .

clean:
	$(MAKE) -C src clean
	rm -f *.so