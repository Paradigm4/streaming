all:
	$(MAKE) -C src
	$(MAKE) -C examples
	@cp src/*.so examples/stream_test_client .

clean:
	$(MAKE) -C src clean
	$(MAKE) -C examples clean
	rm -f *.so
	rm -f stream_test_client