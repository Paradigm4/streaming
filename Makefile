all:
	$(MAKE) -C src
	@cp src/*.so src/stream_test_client .

clean:
	$(MAKE) -C src clean
	rm -f *.so