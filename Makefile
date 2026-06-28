.PHONY: build test clean
build:
	./mvnw clean package -DskipTests

test: build
	./mvnw test

clean:
	$(RM) *.class
