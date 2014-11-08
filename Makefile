all: target/ng.util-0.9.0.jar

target/ng.util-0.9.0.jar:
	lein compile
	lein check
	lein test
	lein jar
	lein install
install:
	lein install
clean:
	lein clean
